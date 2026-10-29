# Quality Scoring JSON Error Fix

## Problem
The quality scoring LLM was returning invalid JSON with errors like:
```
Unterminated string starting at: line 1 column 2348 (char 2347)
```

This occurred when the LLM generated evidence text containing unescaped quotes or special characters, breaking the JSON structure.

## Root Causes
1. **LLM generating malformed JSON**: The LLM sometimes produces JSON with:
   - Unescaped quotes within string values
   - Missing closing quotes, brackets, or braces
   - Trailing commas
   - Newlines within strings

2. **Insufficient prompt guidance**: The original prompts didn't explicitly tell the LLM to avoid quotation marks in evidence descriptions.

3. **No JSON repair mechanism**: The code had no fallback to repair common JSON errors before parsing.

## Solution

### 1. Added JSON Repair Function (`quality.py`)
Created `_repair_json()` function that:
- Removes markdown code block markers
- Removes trailing commas before `}` or `]`
- Escapes unescaped quotes within string values
- Handles newlines within strings
- Closes unclosed strings, arrays, and objects
- Preserves the actual content while fixing structural issues

### 2. Updated Prompt Instructions (`quality.py`)
Modified the LLM prompts to explicitly instruct:
- "avoid quotation marks" in evidence descriptions
- Clearer formatting instructions for JSON output

Changed in two places:
- Conversation-level quality judge prompt (line ~813)
- Per-turn logic check prompt (line ~1193)

### 3. Improved Error Handling (`quality.py`)
Updated `_call_logic_judge()` to:
- First attempt JSON repair before giving up
- Log successful repairs at DEBUG level
- Only log failures with content preview after repair attempt fails

### 4. Fixed Synchronous Fallback Return Value (`generation.py`)
Fixed `_score_and_store_quality()` to:
- Return the quality result dict in synchronous fallback mode (tests)
- Check if result was stored in `app_state.quality_scores` and return it
- Only return `None` in true async mode

### 5. Updated Test Mock (`tests/test_dataset_compatibility.py`)
Added the missing "logic" dimension to the quality mock response to match the current scoring dimensions.

## Testing
All tests pass, including:
- `tests/test_quality_roleplay.py` - Tests roleplay quality dimensions
- `tests/test_dataset_compatibility.py` - Tests dataset binding compatibility
- `tests/test_quality_logic.py` - Tests the logic gate functionality
- Full test suite - All 38 test files pass

## Impact
- **Backward compatible**: Existing functionality unchanged
- **More robust**: Handles malformed LLM output gracefully
- **Better debugging**: Logs when JSON repair succeeds/fails
- **No breaking changes**: All existing tests pass

## Files Modified
1. `/home/user/generator/quality.py` - Added `_repair_json()`, updated prompts, improved error handling
2. `/home/user/generator/generation.py` - Fixed synchronous fallback return value
3. `/home/user/generator/tests/test_dataset_compatibility.py` - Updated mock to include "logic" dimension
