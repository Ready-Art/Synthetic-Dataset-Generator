# core/generation_worker.py
import time
import json
import state
import valkey
from utils.valkey_client import ValkeyClient, ValkeyKeys
from utils.file_io import log_message, write_conversation
from utils.config_handler import save_generation_state
from core.api_handler import generate_question, generate_answer_with_retries, generate_user_continuation

def get_next_system_prompt(prompts_list, valkey_client):
    """
    Cycles through the list of system prompts in a thread-safe manner using Valkey.
    """
    if not prompts_list:
        return "You are a helpful assistant."
    # INCR is atomic, ensuring thread safety
    current_index = valkey_client.incr(ValkeyKeys.SYSTEM_PROMPT_COUNTER)
    return prompts_list[current_index % len(prompts_list)]

def worker(thread_id, worker_params, valkey_connection_kwargs):
    """The main function executed by each worker thread."""
    log_message(f"Thread {thread_id}: Worker started.", "DEBUG")
    
    # Each thread needs its own client instance
    valkey_client = ValkeyClient(**valkey_connection_kwargs)
    if not valkey_client.is_connected():
        log_message(f"Thread {thread_id}: Could not connect to Valkey. Worker exiting.", "ERROR")
        return

    while not state.stop_processing:
        while state.pause_processing and not state.stop_processing:
            time.sleep(0.1)
        if state.stop_processing: break

        try:
            # Blocking pop from Valkey list
            task_json = valkey_client.brpop(ValkeyKeys.TASK_QUEUE, timeout=1)
            if not task_json:
                # Timeout occurred, check if all tasks are done
                if not valkey_client.exists(ValkeyKeys.TASK_QUEUE) and not state.processing_active:
                    break
                continue
            task = json.loads(task_json)
        except valkey.exceptions.ConnectionError:
            log_message(f"Thread {thread_id}: Valkey connection lost. Worker stopping.", "ERROR")
            break
        except Exception as e:
            log_message(f"Thread {thread_id}: Error decoding task from queue: {e}", "ERROR")
            continue

        if state.stop_processing:
            # If stopped, push task back to the queue for next time
            valkey_client.lpush(ValkeyKeys.TASK_QUEUE, json.dumps(task))
            break

        task_id, file_name, *_ = task
        # Check if another thread completed it while it was in transit
        if valkey_client.sismember(ValkeyKeys.COMPLETED_TASK_IDS, task_id):
            log_message(f"Thread {thread_id}: Skipping already completed task {task_id}.", "INFO")
            continue

        try:
            process_single_task(thread_id, task, worker_params, valkey_client)
        except Exception as e:
            import traceback
            log_message(f"Thread {thread_id}: Unhandled error processing task {task_id}: {e}", "ERROR")
            log_message(traceback.format_exc(), "ERROR")

    log_message(f"Thread {thread_id} completed its run.", "INFO")


def process_single_task(thread_id, task, p, valkey_client):
    """Processes a single task from the queue."""
    task_id, file_name, *_ = task
    
    system_prompt = get_next_system_prompt(p['system_prompts'], valkey_client) if p['use_variable_system'] else p['system_prompts'][0]

    api_config_for_task = None
    api_slot_idx_for_task = -1
    if not p['master_duplication_mode']:
        if p['active_apis_for_worker']:
            selected = p['active_apis_for_worker'][thread_id % len(p['active_apis_for_worker'])]
            api_config_for_task = selected['config']
            api_slot_idx_for_task = selected['original_slot_idx']
        else:
            log_message(f"T{thread_id}: No active APIs for non-duplication mode. Skipping.", "ERROR")
            return

    initial_question = _get_initial_question(thread_id, task, system_prompt, p, api_config_for_task, api_slot_idx_for_task, valkey_client)
    if not initial_question:
        log_message(f"T{thread_id}: Failed to generate initial question for task {task_id}. Skipping.", "ERROR")
        return

    llm_context = [{"role": "user", "content": initial_question}]
    output_history = [] if not p['master_duplication_mode'] else None

    if output_history is not None:
        output_history.append(llm_context[0])

    for turn_num in range(p['num_turns']):
        if state.stop_processing: break

        if p['master_duplication_mode']:
            _handle_duplication_turn(thread_id, task_id, turn_num, llm_context, system_prompt, p, valkey_client)
            primary_answer = llm_context[-1]['content'] if llm_context and llm_context[-1]['role'] == 'assistant' else None
            if not primary_answer:
                log_message(f"T{thread_id}: No primary answer to continue conversation for task {task_id}. Ending.", "WARNING")
                break
        else:
            answer = _get_single_answer(thread_id, llm_context, system_prompt, p, api_config_for_task, api_slot_idx_for_task, valkey_client)
            if not answer:
                log_message(f"T{thread_id}: Failed to get answer for task {task_id}, turn {turn_num+1}. Ending.", "ERROR")
                break
            llm_context.append({"role": "assistant", "content": answer})
            output_history.append(llm_context[-1])
            _update_progress(1, p['master_duplication_mode'], valkey_client, api_slot_idx_for_task)

        if turn_num < p['num_turns'] - 1:
            continuation_api_conf = p['all_api_configs'][0] if p['master_duplication_mode'] else api_config_for_task
            continuation_api_idx = 0 if p['master_duplication_mode'] else api_slot_idx_for_task
            
            user_reply = generate_user_continuation(system_prompt, llm_context, p['user_continuation_prompt'], continuation_api_conf, p['max_attempts'], thread_id, continuation_api_idx, valkey_client)
            if not user_reply:
                log_message(f"T{thread_id}: Failed to get user continuation for task {task_id}. Ending.", "ERROR")
                break
            llm_context.append({"role": "user", "content": user_reply})
            if output_history is not None:
                output_history.append(llm_context[-1])

    with state.output_data_lock:
        if not p['master_duplication_mode'] and output_history and len(output_history) >= 2:
            write_conversation(output_history, p['output_format'], task_id, p['master_duplication_mode'])
        
        valkey_client.sadd(ValkeyKeys.COMPLETED_TASK_IDS, task_id)
        save_generation_state(p['global_config'], valkey_client)
        log_message(f"T{thread_id}: Completed task {task_id}.", "INFO")

def _get_initial_question(thread_id, task, system_prompt, p, api_config, api_slot_idx, valkey_client):
    if p['use_questions_file']:
        return task[3]
    else:
        subject, context = task[3], task[4]
        q_gen_api_conf = p['all_api_configs'][0] if p['master_duplication_mode'] else api_config
        q_gen_api_idx = 0 if p['master_duplication_mode'] else api_slot_idx
        return generate_question(system_prompt, p['question_prompt'], subject, context, q_gen_api_conf, p['history_size'], p['max_attempts'], thread_id, q_gen_api_idx, valkey_client)

def _get_single_answer(thread_id, llm_context, system_prompt, p, api_config, api_slot_idx, valkey_client):
    answer_params = {
        'base_system_prompt': system_prompt, 'conversation_history': llm_context,
        'answer_prompt_template': p['answer_prompt'], 'api_config': api_config,
        'api_slot_idx': api_slot_idx, 'thread_id': thread_id, 'max_attempts': p['max_attempts'],
        'refusal_phrases': p['refusal_phrases'], 'user_speaking_phrases': p['user_speaking_phrases'],
        'slop_phrases': p['slop_phrases'], 'jailbreaks': p['jailbreaks'],
        'speaking_fixes': p['speaking_fixes'], 'slop_fixes_fallback': p['slop_fixes_fallback'],
        'no_user_impersonation': p['no_user_impersonation'],
        'slop_fixer_api_config': p['slop_fixer_api_config'], 'max_slop_fix_iters': p['max_slop_fix_iters'],
        'valkey_client': valkey_client
    }
    return generate_answer_with_retries(answer_params)

def _handle_duplication_turn(thread_id, task_id, turn_num, llm_context, system_prompt, p, valkey_client):
    primary_answer = None
    for api_idx, api_conf in enumerate(p['all_api_configs']):
        if api_idx < 4 and api_conf.get('enabled'):
            answer = _get_single_answer(thread_id, llm_context, system_prompt, p, api_conf, api_idx, valkey_client)
            if answer:
                if not primary_answer or api_idx == 0:
                    primary_answer = answer
                
                turn_history = llm_context + [{"role": "assistant", "content": answer}]
                with state.output_data_lock:
                    write_conversation(turn_history, p['output_format'], task_id, p['master_duplication_mode'], api_slot_idx=api_idx, is_duplication_turn=True, turn_number=turn_num + 1)
                _update_progress(1, p['master_duplication_mode'], valkey_client, api_idx)
    
    if primary_answer:
        llm_context.append({"role": "assistant", "content": primary_answer})

def _update_progress(amount, is_duplication, valkey_client, api_idx=None):
    if is_duplication and api_idx is not None:
        valkey_client.incr(ValkeyKeys.progress_per_api(api_idx), amount)
    else:
        valkey_client.incr(ValkeyKeys.PROGRESS_OVERALL, amount)
