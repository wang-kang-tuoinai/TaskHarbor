# 任务跟踪：GitHub Issues

目标仓库：`wang-kang-tuoinai/TaskHarbor`。开发需求、问题和实施任务使用该仓库的 GitHub Issues。

## 操作约定

使用 `gh` CLI。首次写入前，用 `gh repo view --json nameWithOwner` 核对目标仓库；不可访问时报告原因，不改写到其他仓库。

- 发布任务：创建一个 GitHub Issue。
- 获取任务：读取 Issue 正文、标签和评论。
- 多行正文：写入临时文件，通过 `--body-file` 传入。
- 父子任务：优先使用当前工具支持的原生子任务；不支持时在子任务正文写 `Part of #编号`，并在父任务中维护清单。
- 依赖：优先使用原生依赖；不支持时写 `Blocked by: #编号`。所有阻塞任务关闭后才视为解除阻塞。
- 关闭任务：先记录结果及验证情况，再关闭 Issue。
- 分诊角色按 `docs/agents/triage-labels.md` 映射。

## 文档边界

Issue 引用相关操作契约、ADR 和实施计划。`docs/当前进度.md` 保留阶段性进度，不复制全部 Issue 内容。

## Pull requests as a triage surface

PRs as a request surface: no.
