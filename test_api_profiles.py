"""Unit tests for api_profiles.py -- run directly: `python test_api_profiles.py`.

No test framework dependency; each check raises AssertionError on failure and the script exits
non-zero if any fail.
"""
import sys

import api_profiles

FAILURES = []


def check(name, cond):
    if cond:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}")
        FAILURES.append(name)


def full_payload():
    return {
        "model": "m", "messages": [{"role": "user", "content": "hi"}], "stream": False,
        "temperature": 0.5, "top_p": 0.9, "top_k": 50, "min_p": 0.0,
        "repetition_penalty": 1.1, "max_tokens": 128,
        "logit_bias": {"5": 1}, "chat_template_kwargs": {"enable_thinking": False},
    }


def main():
    api_profiles.load_profiles()
    api_profiles.reset_slot_profiles()

    # --- default profile is passthrough and returns the SAME object ---
    p = full_payload()
    out = api_profiles.apply_profile(p, api_profiles.DEFAULT_PROFILE)
    check("default profile is a no-op (same object)", out is p)

    # --- unknown profile name falls back to the default (passthrough) ---
    out = api_profiles.apply_profile(p, "does-not-exist")
    check("unknown profile falls back to passthrough", out is p)

    # --- mistral profile drops non-OpenAI params and maps repetition_penalty ---
    out = api_profiles.apply_profile(full_payload(), "mistral")
    for k in ("top_k", "min_p", "repetition_penalty", "chat_template_kwargs", "logit_bias"):
        check(f"mistral drops {k}", k not in out)
    check("mistral keeps temperature", out.get("temperature") == 0.5)
    check("mistral keeps model/messages/stream",
          all(k in out for k in ("model", "messages", "stream")))
    check("mistral maps repetition_penalty 1.1 -> frequency_penalty 0.1",
          out.get("frequency_penalty") == 0.1)

    # --- openai profile keeps logit_bias but still drops top_k / repetition_penalty ---
    out = api_profiles.apply_profile(full_payload(), "openai")
    check("openai keeps logit_bias", "logit_bias" in out)
    check("openai drops top_k", "top_k" not in out)
    check("openai maps repetition_penalty -> frequency_penalty", out.get("frequency_penalty") == 0.1)

    # --- an explicit frequency_penalty is not overwritten by the rename ---
    pay = full_payload()
    pay["frequency_penalty"] = 1.5
    out = api_profiles.apply_profile(pay, "mistral")
    check("existing frequency_penalty is preserved", out.get("frequency_penalty") == 1.5)

    # --- custom profile: only whitelisted params survive ---
    out = api_profiles.apply_profile(full_payload(), "custom", custom_allowed=["temperature", "top_k"])
    check("custom keeps whitelisted temperature", out.get("temperature") == 0.5)
    check("custom keeps whitelisted top_k", out.get("top_k") == 50)
    check("custom drops non-whitelisted top_p", "top_p" not in out)
    check("custom always keeps messages", "messages" in out)

    # --- custom profile with no list = passthrough ---
    out = api_profiles.apply_profile(full_payload(), "custom", custom_allowed=None)
    check("custom with no param list is passthrough", out.get("top_k") == 50)

    # --- host detection ---
    check("detect api.mistral.ai -> mistral",
          api_profiles.detect_profile("https://api.mistral.ai/v1/chat/completions") == "mistral")
    check("detect apex mistral.ai -> mistral",
          api_profiles.detect_profile("https://mistral.ai/v1/chat/completions") == "mistral")
    check("detect api.openai.com -> openai",
          api_profiles.detect_profile("https://api.openai.com/v1/chat/completions") == "openai")
    check("detect unknown host -> None",
          api_profiles.detect_profile("https://llm.internal.example/v1") is None)
    check("detect lookalike host is not a false positive",
          api_profiles.detect_profile("https://not-mistral.ai.evil.test/v1") is None)

    # --- per-slot registration: explicit profile wins ---
    api_profiles.reset_slot_profiles()
    api_profiles.set_slot_profile(0, "mistral", None, "https://llm.internal.example/v1")
    out = api_profiles.apply_profile_for_slot(full_payload(), 0)
    check("slot 0 explicit mistral profile applied", "top_k" not in out)

    # --- per-slot registration: unset profile + Mistral URL auto-detects ---
    api_profiles.reset_slot_profiles()
    api_profiles.set_slot_profile(1, None, None, "https://api.mistral.ai/v1/chat/completions")
    out = api_profiles.apply_profile_for_slot(full_payload(), 1)
    check("slot 1 auto-detected mistral from URL", "top_k" not in out and out.get("frequency_penalty") == 0.1)

    # --- unregistered slot + no URL -> passthrough (same object) ---
    api_profiles.reset_slot_profiles()
    p = full_payload()
    out = api_profiles.apply_profile_for_slot(p, 3)
    check("unregistered slot with no URL is passthrough", out is p)

    # --- unregistered slot but Mistral URL passed at call time still filters ---
    out = api_profiles.apply_profile_for_slot(full_payload(), 3, "https://api.mistral.ai/v1/chat/completions")
    check("unregistered slot filters via call-time URL detect", "top_k" not in out)

    print()
    if FAILURES:
        print(f"{len(FAILURES)} FAILED: {', '.join(FAILURES)}")
        sys.exit(1)
    print("all api_profiles tests passed")


if __name__ == "__main__":
    main()
