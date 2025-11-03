# utils/text_processing.py
import re
import state
from utils.valkey_client import ValkeyKeys

def update_question_history(question, history_size, valkey_client):
    """Adds a question to the history list in Valkey and trims it."""
    valkey_client.push_to_list_with_trim(ValkeyKeys.QUESTION_HISTORY, question, history_size)

def is_refusal(answer, refusal_phrases):
    """Detects if the LLM's answer contains refusal phrases."""
    return _detect_phrases_in_sentences(answer, refusal_phrases)

def is_user_speaking(answer, user_speaking_phrases):
    """Detects if the assistant's answer impersonates the user."""
    return _detect_phrases_in_sentences(answer, user_speaking_phrases)

def is_slop(answer, slop_phrases):
    """Detects if the answer contains "slop" (undesirable phrases)."""
    return _detect_phrases_in_sentences(answer, slop_phrases)

def _detect_phrases_in_sentences(text, phrases_to_detect):
    """Generic helper to detect a list of phrases within the sentences of a text block."""
    if not text or not phrases_to_detect:
        return False, []
        
    # Split text into sentences while keeping delimiters
    sentences = re.split(r'([.!?\n]+)', text)
    original_sentences = [sentences[i] + (sentences[i+1] if i + 1 < len(sentences) else '') for i in range(0, len(sentences), 2)]
    
    detected_info = []
    for phrase in phrases_to_detect:
        if not phrase.strip(): continue
        # Use word boundaries for more accurate matching
        pattern = r'\b' + re.escape(phrase.strip()) + r'\b'
        for sentence in original_sentences:
            if re.search(pattern, sentence, re.IGNORECASE):
                detected_info.append((phrase, sentence.strip()))
                break # Move to the next phrase once found
    return bool(detected_info), detected_info

def remove_reasoning_text(text):
    """Removes <think>...</think> style reasoning blocks from text."""
    if re.search(r'<think>', text, re.IGNORECASE) and not re.search(r'</think>', text, re.IGNORECASE):
        return None  # Invalid output, reject it
    
    match = re.search(r'</think>\s*(.*)', text, re.DOTALL | re.IGNORECASE)
    return match.group(1).strip() if match else text
