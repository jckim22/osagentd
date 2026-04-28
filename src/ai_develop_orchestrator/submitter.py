from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import uuid
from textwrap import shorten
from pathlib import Path
from typing import Dict, Iterable, List

from .cli import request
from .daemon import DEFAULT_SOCKET_PATH


def _csv(raw: str) -> List[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Interactive task submitter for osagentd")
    parser.add_argument("--socket-path", default=os.environ.get("AIDO_SOCKET_PATH", DEFAULT_SOCKET_PATH))
    parser.add_argument("--default-capabilities", default="code,python")
    parser.add_argument("--default-priority", type=int, default=50)
    parser.add_argument("--default-type", default="codex-task")
    parser.add_argument("--default-labels", default="")
    parser.add_argument("--default-workdir", default=os.getcwd())
    parser.add_argument("--worker-window", default="workers")
    return parser.parse_args()


PIPELINE_STAGES = [
    ("plan", ["plan", "analysis"]),
    ("research", ["research", "analysis"]),
    ("code", ["code", "python"]),
    ("review", ["review", "code"]),
    ("merge", ["analysis"]),
]


STAGE_PROMPTS = {
    "plan": {
        "mission": "Turn the original request into an execution strategy for the rest of the team.",
        "brief": "You own planning only. Produce a compact handoff that helps code/review workers avoid wandering.",
        "do": [
            "Clarify the goal in your own words.",
            "Identify likely files, commands, risks, and dependencies.",
            "Break the work into small steps another worker can follow.",
            "Call out any ambiguity that could change the implementation.",
        ],
        "dont": [
            "Do not edit files.",
            "Do not run long or destructive commands.",
            "Do not duplicate the researcher's deep code walkthrough.",
        ],
        "output": [
            "Plan summary",
            "Suggested file/area checklist",
            "Risk list",
            "Recommended next action for coder",
        ],
    },
    "research": {
        "mission": "Inspect the workspace and collect facts needed to do the work safely.",
        "brief": "You own context discovery only. Your output should be factual and useful as input to the coder.",
        "do": [
            "Search the repository for relevant entrypoints, configs, tests, and docs.",
            "Summarize current behavior and constraints with concrete file references.",
            "Report commands you ran and notable outputs.",
            "Surface unknowns or missing context.",
        ],
        "dont": [
            "Do not edit files.",
            "Do not propose broad rewrites.",
            "Do not spend time on UX polish unless the request is UX-specific.",
        ],
        "output": [
            "Relevant files",
            "Current behavior",
            "Important constraints",
            "Facts the coder/reviewer should know",
        ],
    },
    "code": {
        "mission": "Make the smallest safe implementation change using the planner/researcher handoff.",
        "brief": "You own implementation. Use upstream context if provided, then edit/verify the workspace.",
        "do": [
            "Inspect the workspace just enough to avoid blind edits.",
            "Edit files directly when a code/doc/config change is needed.",
            "Keep the patch focused and compatible with the existing style.",
            "Run fast verification when available.",
        ],
        "dont": [
            "Do not wait for the planner/researcher; make reasonable local decisions.",
            "Do not perform destructive git operations.",
            "Do not rewrite unrelated files.",
        ],
        "output": [
            "Changed files",
            "Behavior changed",
            "Verification performed",
            "Remaining risks or TODOs",
        ],
    },
    "review": {
        "mission": "Review the coder's result as a critical reviewer.",
        "brief": "You own verification. Use upstream context if provided, but focus on bugs, regressions, and ship risk.",
        "do": [
            "Look for bugs, regressions, missing tests, race conditions, and UX confusion.",
            "Prioritize findings by severity.",
            "Reference concrete files/lines when possible.",
            "Say explicitly if you find no blocking issues.",
        ],
        "dont": [
            "Do not make broad edits unless the fix is tiny and obvious.",
            "Do not summarize before findings.",
            "Do not rubber-stamp uncertain behavior.",
        ],
        "output": [
            "Findings first",
            "Open questions",
            "Suggested verification",
            "Ship/no-ship recommendation",
        ],
    },
    "merge": {
        "mission": "Synthesize all worker outputs into one final operator-facing result.",
        "brief": "You own integration. Do not redo the work. Merge plan, research, code, and review outputs into a concise final answer.",
        "do": [
            "Read the upstream worker outputs injected by osagentd.",
            "Resolve disagreements or call them out explicitly.",
            "Summarize what was done, what changed, and what remains.",
            "Give the user a clear next action.",
        ],
        "dont": [
            "Do not edit files.",
            "Do not repeat every raw log line.",
            "Do not hide failed or missing upstream work.",
        ],
        "output": [
            "Final integrated summary",
            "Role-by-role contribution summary",
            "Verification and risks",
            "Recommended next step",
        ],
    },
}


def _discover_targets(default_workdir: str) -> Dict[str, str]:
    base = Path(default_workdir)
    targets: Dict[str, str] = {}
    if not base.exists():
        return targets
    for child in sorted(base.iterdir()):
        if not child.is_dir():
            continue
        if child.name.startswith("."):
            continue
        targets[child.name] = str(child.resolve())
    return targets


def _rule(char: str = "=") -> str:
    width = shutil.get_terminal_size((100, 40)).columns
    return char * max(40, min(width - 1, 120))


def _render_screen(state: Dict[str, object], last_message: str = "", mode: str = "READY") -> str:
    targets_preview = ", ".join(list(state["targets"].keys())[:6]) or "-"
    pipeline = "ON - 한 문장을 planner/researcher/coder/reviewer로 분해" if state["pipeline"] else "OFF - 단일 Codex worker 작업"
    lines = [
        _rule("="),
        f"osagentd submitter  |  {mode}",
        _rule("="),
        "",
        "여기에 자연어로 할 일을 쓰고 Enter를 누르세요.",
        "예: README 정리하고 실행 UX 더 친절하게 만들어줘",
        "",
        f"pipeline : {pipeline}",
        "workers  : Enter를 치면 workers 창으로 자동 이동해서 4분할 실시간 로그를 보여줍니다.",
        "focus    : 이 pane에 입력합니다. tmux 이동은 Ctrl-b + 방향키 또는 마우스 클릭.",
        "",
        _rule("-"),
        "CURRENT TARGET",
        _rule("-"),
        f"caps      : {','.join(state['caps']) or '-'}",
        f"resources : {shorten(','.join(state['resources']) or '-', width=72, placeholder='..')}",
        f"priority  : {state['priority']}",
        f"type      : {state['task_type']}",
        f"labels    : {','.join(state['labels']) or '-'}",
        f"workdir   : {shorten(str(state['workdir']), width=72, placeholder='..')}",
        f"targets   : {shorten(targets_preview, width=72, placeholder='..')}",
        "",
        _rule("-"),
        "FAST COMMANDS",
        _rule("-"),
        "/target repo-name     choose repo under your develop folder",
        "/targets              show detected repos",
        "/workdir /path/repo    choose any repo manually",
        "/pipeline on|off       fan-out roles or submit one task",
        "/quit                 close this input pane   |   Ctrl-b p returns from workers",
        "",
        _rule("-"),
        "LAST EVENT",
        _rule("-"),
        shorten(last_message or "none", width=100, placeholder=".."),
        "",
        _rule("-"),
        "INPUT",
        _rule("-"),
        "Prompt:",
        "",
    ]
    return "\n".join(lines)


def _event(message: str) -> None:
    print(f"\n[event] {message}", flush=True)


def _switch_to_workers(worker_window: str) -> None:
    if not os.environ.get("TMUX"):
        return
    session = subprocess.run(
        ["tmux", "display-message", "-p", "#S"],
        text=True,
        capture_output=True,
        check=False,
    ).stdout.strip()
    if not session:
        return
    subprocess.run(
        ["tmux", "switch-client", "-t", f"{session}:{worker_window}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


def _stage_prompt(stage: str, user_prompt: str, workdir: str, run_id: str, resources: List[str]) -> str:
    spec = STAGE_PROMPTS[stage]
    resources_text = ", ".join(resources) if resources else "(none specified)"
    return "\n".join(
        [
            f"osagentd run: {run_id}",
            f"role: {stage}",
            f"workdir: {workdir}",
            f"declared resources: {resources_text}",
            "",
            "You are one worker in an OS-native multi-agent development run.",
            "Your prompt is intentionally specialized. Stay in your lane so the team avoids duplicated effort.",
            "",
            f"MISSION: {spec['mission']}",
            f"YOUR TASK BRIEF: {spec['brief']}",
            "",
            "DO:",
            *[f"- {item}" for item in spec["do"]],
            "",
            "DO NOT:",
            *[f"- {item}" for item in spec["dont"]],
            "",
            "OUTPUT CONTRACT:",
            *[f"- {item}" for item in spec["output"]],
            "",
            "ORIGINAL USER REQUEST (context only; follow your role-specific brief above):",
            user_prompt,
        ]
    )


def _submit_one(args: argparse.Namespace, state: Dict[str, object], prompt: str) -> Dict[str, object]:
    return request(
        args.socket_path,
        {
            "action": "submit_task",
            "task_type": state["task_type"],
            "priority": state["priority"],
            "required_capabilities": state["caps"],
            "required_resources": state["resources"],
            "labels": state["labels"],
            "payload": {
                "prompt": prompt,
                "source": "submitter-pane",
                "workdir": state["workdir"],
            },
        },
    )


def _submit_pipeline(args: argparse.Namespace, state: Dict[str, object], prompt: str) -> List[str]:
    run_id = uuid.uuid4().hex[:8]
    task_ids: List[str] = []
    ids_by_stage: Dict[str, str] = {}
    base_priority = int(state["priority"])
    for index, (stage, caps) in enumerate(PIPELINE_STAGES):
        dependencies: List[str] = []
        if stage == "code":
            dependencies = [ids_by_stage[name] for name in ("plan", "research") if name in ids_by_stage]
        if stage == "review" and "code" in ids_by_stage:
            dependencies = [ids_by_stage["code"]]
        if stage == "merge":
            dependencies = [
                ids_by_stage[name]
                for name in ("plan", "research", "code", "review")
                if name in ids_by_stage
            ]
        labels = list(state["labels"]) + [f"run:{run_id}", f"stage:{stage}"]
        resp = request(
            args.socket_path,
            {
                "action": "submit_task",
                "task_type": stage,
                "priority": base_priority + (len(PIPELINE_STAGES) - index),
                "required_capabilities": caps,
                "required_resources": state["resources"],
                "labels": labels,
                "dependencies": dependencies,
                "payload": {
                    "prompt": _stage_prompt(stage, prompt, str(state["workdir"]), run_id, list(state["resources"])),
                    "source": "submitter-pane",
                    "workdir": state["workdir"],
                    "run_id": run_id,
                    "stage": stage,
                    "task_brief": STAGE_PROMPTS[stage]["brief"],
                    "original_prompt": prompt,
                },
            },
        )
        if resp.get("ok"):
            task_id = str(resp.get("task_id", ""))
            ids_by_stage[stage] = task_id
            task_ids.append(task_id[:8])
        else:
            task_ids.append(f"failed:{stage}")
    return [run_id, *task_ids]


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args()
    state = {
        "caps": _csv(args.default_capabilities),
        "priority": args.default_priority,
        "task_type": args.default_type,
        "labels": _csv(args.default_labels),
        "resources": [],
        "workdir": args.default_workdir,
        "targets": _discover_targets(args.default_workdir),
        "pipeline": True,
    }
    last_message = "submitter ready"
    print(_render_screen(state, last_message=last_message), flush=True)
    while True:
        try:
            raw = input("task> ").strip()
        except EOFError:
            return 0
        except KeyboardInterrupt:
            print()
            return 0
        if not raw:
            continue
        if raw.startswith("/"):
            cmd, _, value = raw.partition(" ")
            if cmd == "/quit":
                return 0
            if cmd == "/help":
                last_message = "help: write a prompt, or use /target /workdir /pipeline /resources /priority"
                _event(last_message)
                continue
            if cmd == "/show":
                last_message = "show: defaults refreshed"
                print(_render_screen(state, last_message=last_message), flush=True)
                continue
            if cmd == "/targets":
                names = ", ".join(state["targets"].keys()) or "-"
                last_message = f"targets -> {names}"
                _event(last_message)
                continue
            if cmd == "/caps":
                state["caps"] = _csv(value)
                last_message = f"updated caps -> {','.join(state['caps']) or '-'}"
                _event(last_message)
                continue
            if cmd == "/resources":
                state["resources"] = _csv(value)
                last_message = f"updated resources -> {','.join(state['resources']) or '-'}"
                _event(last_message)
                continue
            if cmd == "/priority":
                state["priority"] = int(value.strip())
                last_message = f"updated priority -> {state['priority']}"
                _event(last_message)
                continue
            if cmd == "/workdir":
                state["workdir"] = value.strip() or args.default_workdir
                last_message = f"updated workdir -> {state['workdir']}"
                _event(last_message)
                continue
            if cmd == "/pipeline":
                desired = value.strip().lower()
                state["pipeline"] = desired not in {"off", "false", "0", "no"}
                last_message = f"pipeline -> {'on' if state['pipeline'] else 'off'}"
                _event(last_message)
                continue
            if cmd == "/target":
                target_name = value.strip()
                target_path = state["targets"].get(target_name)
                if target_path:
                    state["workdir"] = target_path
                    last_message = f"target selected -> {target_name} ({target_path})"
                else:
                    last_message = f"unknown target -> {target_name}"
                _event(last_message)
                continue
            if cmd == "/type":
                state["task_type"] = value.strip() or "codex-task"
                last_message = f"updated type -> {state['task_type']}"
                _event(last_message)
                continue
            if cmd == "/labels":
                state["labels"] = _csv(value)
                last_message = f"updated labels -> {','.join(state['labels']) or '-'}"
                _event(last_message)
                continue
            last_message = "unknown command; use /help"
            _event(last_message)
            continue

        if state["pipeline"]:
            run_parts = _submit_pipeline(args, state, raw)
            last_message = f"submitted run {run_parts[0]} -> tasks {', '.join(run_parts[1:])} | switching to workers"
            _event(last_message)
            _switch_to_workers(args.worker_window)
        else:
            resp = _submit_one(args, state, raw)
            if resp.get("ok"):
                last_message = f"submitted task {resp.get('task_id')}"
            else:
                last_message = f"submit failed: {resp}"
            _event(last_message)
            _switch_to_workers(args.worker_window)


if __name__ == "__main__":
    raise SystemExit(main())
