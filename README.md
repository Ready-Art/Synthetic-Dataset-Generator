# Synthetic-Dataset-Generator
A robust, GUI-driven Python application designed for generating high-quality synthetic conversation datasets for Large Language Model (LLM) training. This tool features multi-API orchestration, advanced issue detection (refusals, slop, user impersonation), character persona injection, and crash recovery capabilities.

Features:

Multi-API Support: Configure up to 6 API slots. Slots 1-4 are used for main generation (with optional master duplication mode), Slot 5 for Slop Fixing, and Slot 6 for Anti-Slop Fixing

Issue Detection & Mitigation: Automatically detects and attempts to fix refusals, user speaking patterns, slop (undesirable phrases), and anti-slop issues during generation

Character Engine: Injects dynamic character profiles (name, job, appearance, backstory) and emotional states into conversations to enhance diversity

Crash Recovery: Saves generation state to JSON, allowing users to resume interrupted tasks without losing progress

Rate Limiting: Built-in rate limiter per API slot to prevent exceeding provider limits

Caching: Supports Valkey (Redis) for caching LLM responses to reduce costs and latency

GUI Configuration: A comprehensive Tkinter-based editor for managing all settings, including prompts, samplers, and API keys

Output Formats: Supports ShareGPT and OpenAI-like JSONL formats

Released under  the MIT license.
