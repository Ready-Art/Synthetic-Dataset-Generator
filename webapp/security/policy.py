# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""The password policy.

**Enforced when a password is SET, not only when someone logs in with one.** A weak password
therefore can never be stored, which is the only version of this rule that matters: a login-time
check on an already-weak hash protects nothing.

No external dependency. Six rules, in the order they are cheapest to check:

1. Length: at least 12 characters, at most 1024. The cap is not cosmetic -- argon2 is
   deliberately slow, so an unbounded password is a hashing DoS.
2. Not in the common-password list, which includes the app's own vocabulary
   (``readyart``, ``synthetic``, ``dataset``, ``generator``, ``chatgpt``, ``llama``,
   ``openai``) because those are the first thing anyone would try on this app.
3. Not one repeated character (``aaaaaaaaaaaa``).
4. Not a keyboard or alphabet sequence (``qwerty``, ``asdf``, ``0123456789``, ``abcdef…``).
5. At least two character classes (lower / upper / digit / other).
6. Not the app name plus a few digits (``ReadyArt2026``).

The ten cases in :data:`VERIFIED_CASES` are the plan's own table, and
``tests/test_web_auth.py`` runs every one of them.
"""

import re

MIN_LENGTH = 12
MAX_LENGTH = 1024

# Words that must never be the whole password. The app's own vocabulary is here on purpose:
# on a deployment of this tool, "syntheticdataset" is the first guess anyone makes.
COMMON_PASSWORDS = frozenset({
    'password', 'passw0rd', 'password1', 'password123', 'passphrase',
    'letmein', 'welcome', 'admin', 'administrator', 'root', 'toor',
    'qwerty', 'qwertyuiop', 'asdfghjkl', 'zxcvbnm', 'iloveyou',
    'monkey', 'dragon', 'sunshine', 'princess', 'football', 'baseball',
    'trustno1', 'abc123', '123456', '12345678', '123456789', '1234567890',
    '111111', '000000', 'changeme', 'secret', 'default', 'guest',
    'login', 'master', 'shadow', 'superman', 'batman', 'starwars',
    # App-specific: this tool's own name and the things it talks to.
    'readyart', 'synthetic', 'dataset', 'generator', 'syntheticdataset',
    'syntheticdatasetgenerator', 'readyartgenerator', 'chatgpt', 'llama',
    'openai', 'vllm', 'huggingface',
})

# Keyboard rows, walked forwards and backwards, plus the alphabet and the digits. A password
# that is one of these with a couple of characters appended is still a sequence.
_KEYBOARD_ROWS = (
    'qwertyuiop', 'asdfghjkl', 'zxcvbnm',
    '1234567890', 'abcdefghijklmnopqrstuvwxyz',
)

# How long a run of one keyboard row or the alphabet disqualifies a password.
# 8 catches "0123456789ab" and "abcdefghijkl" without rejecting a passphrase that happens
# to contain "abc" or "123".
_SEQUENCE_RUN = 8

# The app name plus a few digits, e.g. "ReadyArt2026" or "readyart1234".
_APP_NAME_DIGITS_RE = re.compile(
    r'^(?:readyart|synthetic|dataset|generator)[\W_]*\d{1,6}[\W_]*$', re.IGNORECASE)


class PolicyError(ValueError):
    """A password that the policy rejects. ``str(exc)`` is safe to show the user."""


def _character_classes(password):
    """How many of lower / upper / digit / other the password uses."""
    classes = 0
    if any(character.islower() for character in password):
        classes += 1
    if any(character.isupper() for character in password):
        classes += 1
    if any(character.isdigit() for character in password):
        classes += 1
    if any(not character.isalnum() for character in password):
        classes += 1
    return classes


def _is_single_repeated_character(password):
    return len(set(password)) == 1


def _is_sequence(password):
    """True when the password is (essentially) one keyboard row or the alphabet.

    Checked on the lowercased, non-alphanumeric-stripped password, so ``Qwerty!@#$%^&*()`` is
    still caught: the punctuation does not make it a secret.

    The test is a **run** test, not a whole-string test, because ``0123456789ab`` is a digit
    sequence with two letters glued on and is just as guessable as ``0123456789``. Any run of
    ``_SEQUENCE_RUN`` characters that appears in a row (forwards or backwards) disqualifies.
    """
    stripped = ''.join(character for character in password.lower()
                       if character.isalnum())
    if len(stripped) < _SEQUENCE_RUN:
        return False
    for start in range(len(stripped) - _SEQUENCE_RUN + 1):
        window = stripped[start:start + _SEQUENCE_RUN]
        for row in _KEYBOARD_ROWS:
            if window in row or window in row[::-1]:
                return True
    return False


def _is_common(password):
    lowered = password.lower()
    if lowered in COMMON_PASSWORDS:
        return True
    # "password1234" is not in the list but is just as guessable: strip trailing digits and
    # punctuation and test the stem too.
    stem = re.sub(r'[\W_\d]+$', '', lowered)
    return bool(stem) and stem in COMMON_PASSWORDS


def validate(password):
    """Return ``None`` when ``password`` is acceptable, else raise :class:`PolicyError`.

    The message names the rule that failed, because a policy that only says "too weak" makes a
    user guess and then give up.
    """
    if not isinstance(password, str):
        raise PolicyError('The password must be text.')
    if len(password) < MIN_LENGTH:
        raise PolicyError(
            f'The password must be at least {MIN_LENGTH} characters '
            f'(this one is {len(password)}).')
    if len(password) > MAX_LENGTH:
        raise PolicyError(f'The password must be at most {MAX_LENGTH} characters.')
    if _is_single_repeated_character(password):
        raise PolicyError('The password cannot be one repeated character.')
    if _is_common(password):
        raise PolicyError('That password is too common. Pick something less guessable.')
    if _is_sequence(password):
        raise PolicyError('The password cannot be a keyboard or alphabet sequence.')
    if _APP_NAME_DIGITS_RE.match(password):
        raise PolicyError('The password cannot be the application name plus a few digits.')
    if _character_classes(password) < 2:
        raise PolicyError(
            'The password must mix at least two of: lowercase, uppercase, digits, symbols.')
    return None


def is_acceptable(password):
    """``True``/``False`` without raising. For a UI that wants to disable a button."""
    try:
        validate(password)
    except PolicyError:
        return False
    return True


def describe_rules():
    """The rules as short sentences, for the setup page and the login hint."""
    return (
        f'At least {MIN_LENGTH} characters.',
        'Not a common password, and not this application\'s own name.',
        'Not one repeated character, and not a keyboard or alphabet sequence.',
        'At least two of: lowercase, uppercase, digits, symbols.',
    )


# The verified table, as (password, expected_acceptable). The test asserts every row.
VERIFIED_CASES = (
    ('short', False),
    ('password', False),
    ('123456789012', False),
    ('aaaaaaaaaaaa', False),
    ('qwertyuiop12', False),
    ('alllowercaseletters', False),
    ('ReadyArt2026', False),
    ('correct horse battery staple', True),
    ('Tr0ub4dor&3xK', True),
    ('my-S3cure-passphrase', True),
)
