# 每晚代码质量巡检（V1.1）

[English](nightly-code-quality-review-v1.1.md) | 中文

本页是定时任务「每晚代码质量巡检」的可复用 Prompt 基线 V1.1，适配当前已合入 Desktop 的定时任务能力。它回答「如何定时在夜间巡检当天合入目标分支的代码增量、跑工程门禁、产出可复核报告」，并如实标注当前产品尚未支持的能力。

## 使用说明

1. 新建定时任务，选「代码质量检查」模板。
2. 计划选 `Daily`，建议 00:30；执行位置 `local`（Cloud 尚未开放）。
3. 目标选一个 Git 项目；智能体选能跑命令、也能做代码审查的模型。
4. 把下方「Prompt（V1.1 原文）」整段填入 prompt 字段。
5. 勾选 `full_auto_in_worktree` 无人值守同意。
6. 保存后核对 `nextRunAt`。**local 只在 Desktop 运行时触发**：凌晨未开则那次记 `missed`、不补跑。

## 当前产品限制

- **时区**：local 任务不提供时区选择，schedule 按系统当前时区求值；Prompt 会在报告里记录实际时区。
- **超时**：每次运行固定 60 分钟、不可配，超时记 `failed(timeout)`。
- **运行终态**：`run.status` / `failure_reason` 由执行层决定，Prompt **不能**改写；业务结论写在最终响应的 `QA_VERDICT:` 哨兵里，由 runtime 解析成独立 verdict 字段（`succeeded` 的运行可独立携带 `fail`）。
- **产物持久化**：写在 worktree 根下 `.ora-scheduled-artifacts/` 目录内的白名单文件会在收尾时被采集，脱离 worktree 生命周期持久化，随运行历史列出并按 Run ID 访问。
- **增量范围的近似性**：commit 的提交时间不等于实际合入时间；用 first-parent 时间窗口圈定「上一自然日」是当前近似方案，边缘时刻可能漏记或迟记，Prompt 用 `QA_VERDICT: range_unresolved` 明确标注无法可信确定范围的情形。
- **漏跑不自动补偿**：local 未运行期间的到期点记 `missed`，不集中补跑。

## Prompt（V1.1 原文）

### 目标

对目标 Git 仓库上一自然日合入目标分支的代码增量做无人值守质量巡检，发现潜在缺陷、运行工程门禁，产出可复核的质量报告。

**默认仅审查，不修改、不提交、不推送、不建 PR。**

### 执行前提（产品能力边界）

- 计划：`Daily`，建议 00:30。
- 时区：按系统当前时区（local 不提供时区选择），报告里记录实际时区。
- 环境：独立隔离 worktree（干净 checkout 于任务 base 分支）。
- 超时：固定 60 分钟，不可配。
- 终态：run 终态由执行层决定，与业务结论无关——会话正常结束即 `succeeded`，到点 `failed(timeout)`。本 Prompt 的业务结论只能写进最终响应，不得宣称能改写 run.status。

### Step 0 — 环境检查

1. 确认仓库、目标分支、远程地址与当前 SHA；记录执行日期、实际系统时区、Agent/模型版本。
2. `git fetch origin`，失败 → 记 `QA_VERDICT: blocked` 并 STOP。
3. 不触碰用户原始工作目录；不改目标分支；复用仓库已有 QA/CI 指令。

### Step 1 — 确定增量范围（关键）

用远程目标分支快照定位基准，禁止用 `git log --since "24h"` 滚动窗口代替。

在 `origin/<base_branch>` 的 first-parent 历史上，用提交的提交时间：

1. 找上一自然日 00:00 之前的最后一个提交 = `BASE_SHA`。
2. 找当日 00:00 之前的最后一个提交 = `TARGET_SHA`。
3. 校验 `BASE_SHA` 是 `TARGET_SHA` 祖先。
4. 用 `git diff BASE_SHA..TARGET_SHA` 提取真实增量；输出文件数、增删行、提交数、作者、主要模块。

无增量 → `QA_VERDICT: idle` STOP；缺基准/异常历史 → `QA_VERDICT: range_unresolved` STOP，不得猜范围。

### Step 2 — 增量审查

维度：正确性、安全性、并发、资源管理、性能、兼容性、可观测性。

每条 Finding 必含：Severity(Critical/High/Medium/Low)、文件+行号、相关 commit、问题与触发条件、影响、证据、修复建议、置信度。不凭推测定「已确认缺陷」。

### Step 3 — 工程门禁（60 分钟内优先高价值项）

在 TARGET_SHA 快照上按序执行，优先增量/已有缓存结果，避免全量重跑：

1. 构建/编译
2. 关键单元测试（增量优先）
3. lint / 静态检查
4. type check（适用）
5. `git diff --check BASE_SHA..TARGET_SHA` — 检查已解析比较范围内已提交内容的空白错误；在干净的 checkout 上运行裸 `git diff --check` 只会比较空的工作区差异，可能误报通过

记录命令、退出码、通过/失败数。SKIPPED ≠ PASS；环境/依赖不足、超时必须与真实代码失败区分。为最终报告预留时间。

### Step 4 — 最终响应（必须独立完整，可从运行历史复核）

不要依赖 worktree 临时文件长期保存——结论必须完整写进最终响应：

- 审查日期 + 实际系统时区
- 仓库、分支、`BASE_SHA` / `TARGET_SHA`
- 变更摘要
- Findings（按 Severity 排序）
- 构建/测试/lint 结果
- 未执行项及原因
- `QA_VERDICT`
- Blockers 与后续行动

### Step 5 — QA_VERDICT

在最终响应里单独成行，按冻结哨兵 `QA_VERDICT: <pass|warn|fail|blocked|idle|range_unresolved>` 输出，可紧随一行 `QA_DETAIL: <自由文本>`：

- `QA_VERDICT: pass`：完成、必需门禁通过、无 Critical/High。
- `QA_VERDICT: warn`：无阻断，但有 Medium/Low 或非关键检查缺失。
- `QA_VERDICT: fail`：确认 Critical/High，或必需门禁失败。
- `QA_VERDICT: blocked`：环境/依赖/超时阻止完成审查。
- `QA_VERDICT: range_unresolved`：无法可信确定比较范围。
- `QA_VERDICT: idle`：无增量。

输出任一 verdict 后 STOP，不进入修复/发布/合并。
