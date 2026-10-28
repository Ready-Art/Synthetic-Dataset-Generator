"""Standalone validation for the random_settings / ensemble-unification logic.

Mirrors the exact algorithm added to worker() in generation.py so we can exercise
the three behaviours without standing up the full GUI/worker stack:
  1. empty character setting  -> sampled from the random_settings pool (when enabled)
  2. multi-character ensemble -> unified to ONE shared setting picked from the ensemble
  3. setting selection off    -> standard indoor environment default
"""
import random


def resolve_settings(selected_chars, enable_setting_selection,
                     enable_random_settings, random_settings_list):
    """Copy of the resolution block inserted into worker()."""
    default_setting = 'A standard indoor environment.'
    settings_for_selected = [default_setting] * len(selected_chars)
    if enable_setting_selection:
        per_char_settings = []
        for c in selected_chars:
            own_setting = str(c.get('setting', '') or '').strip()
            if own_setting:
                per_char_settings.append(own_setting)
            elif enable_random_settings and random_settings_list:
                per_char_settings.append(random.choice(random_settings_list))
            else:
                per_char_settings.append(default_setting)
        if len(selected_chars) > 1:
            distinct_settings = list(dict.fromkeys(per_char_settings))
            unified_setting = random.choice(distinct_settings)
            settings_for_selected = [unified_setting] * len(selected_chars)
        else:
            settings_for_selected = per_char_settings
    return settings_for_selected


POOL = ["A quiet park bench", "A late-night diner", "A crowded subway car"]


def run():
    # 1. Single char, own setting present -> kept (no unification)
    chars = [{"name": "A", "setting": "A hospital ward"}]
    assert resolve_settings(chars, True, True, POOL) == ["A hospital ward"]

    # 2. Single char, empty setting, pool enabled -> drawn from pool (or default)
    random.seed(1)
    chars = [{"name": "A", "setting": ""}]
    out = resolve_settings(chars, True, True, POOL)
    assert out[0] in (POOL + ['A standard indoor environment.']), out

    # 3. Two chars, both empty, pool enabled -> UNIFIED to a single shared setting
    random.seed(2)
    chars = [{"name": "A", "setting": ""}, {"name": "B", "setting": ""}]
    out = resolve_settings(chars, True, True, POOL)
    assert len(set(out)) == 1, out          # one shared setting
    assert out[0] in POOL + ['A standard indoor environment.'], out

    # 4. Two chars, mixed own+empty -> unified to one member of the ensemble
    random.seed(3)
    chars = [{"name": "A", "setting": "A hospital ward"},
             {"name": "B", "setting": ""}]
    out = resolve_settings(chars, True, True, POOL)
    assert len(set(out)) == 1, out
    assert out[0] in ("A hospital ward",) + tuple(POOL), out

    # 5. Setting selection OFF -> always the standard default
    chars = [{"name": "A", "setting": "A hospital ward"},
             {"name": "B", "setting": ""}]
    out = resolve_settings(chars, False, True, POOL)
    assert out == ['A standard indoor environment.'] * 2, out

    # 6. Pool disabled, empty settings, single char -> default
    chars = [{"name": "A", "setting": ""}]
    out = resolve_settings(chars, True, False, [])
    assert out == ['A standard indoor environment.'], out

    # Determinism of unification choice across the seed range stays within ensemble
    for s in range(50):
        random.seed(s)
        chars = [{"name": "A", "setting": "own A"},
                 {"name": "B", "setting": "own B"},
                 {"name": "C", "setting": ""}]
        out = resolve_settings(chars, True, True, POOL)
        assert len(set(out)) == 1, out
        assert out[0] in ("own A", "own B") + tuple(POOL), out

    print("All random_settings resolution tests passed.")


if __name__ == "__main__":
    run()
