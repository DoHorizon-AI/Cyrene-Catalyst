# Startup hold validation / 启动暂停验证

The executable change is frozen at `0f4fb7ce12678785b10b69cd3c1d1cf30cfa4499`.
The adjacent [machine receipt](restore-hold-validation.json) records exact source
and evidence hashes. Evidence is local validation with synthetic data; no hosted
CI, deployment, real model, account or vendor acceptance is claimed.

The locked Catalyst environment uses Runtime 0.2.0. The full suite passed all
129 tests, including 21 startup-hold tests; Ruff, formatting and mypy passed.
The first canonical-export run had 128 passes and one environment failure:
the boundary subprocess required a Git index and `python` on PATH. Its negative
log is retained. An empty local index of canonical files and the owned venv PATH
fixed that fixture environment; the original boundary and full suite then passed.
No upstream commit history was synthesized, and no assertions were weakened.

An additional real OS-process proof uses the official knowledge preparation
Plugin and Artifact SDK with Runtime 0.2.2. A canonical queued external
`BUILD_KNOWLEDGE` run cannot start under malformed, directory or dangling
restore markers: each case has zero provider calls and unchanged database,
schema, nonce and file bytes. Normal unmarked startup completes the queued run
with one official Plugin call and two actual artifacts. App/coordinator code is
not patched. The first input-fixture attempt lacked a required locator and failed
provider validation; that log is retained separately from the corrected proof.

The final affected integration delta uses fresh canonical exports and fresh
state with Navigator `0db5deab1560b41f936d81a8f8ae5154fe88148b`, Client
`abd70a7ccdaf0cc20c5931206f370cd16b3e82ca`, and the guard commit above.
Actual Chromium/React/Control/WebHost/Executor/SQLite, Catalyst, preparation,
Artifact and retrieval services passed 16 browser gates. Two actors see and
select their own catalog packages plus a shared package; the other private
package is denied. Their actual retrieved bodies enter separate authorized
model contexts. An actual Catalyst source edit (201) and approval (200) revoke
Alice's input before the synthetic model response; the Task fails with zero
active assistant messages. Cross-actor conversation, memory, persona, profile,
graph and attachment checks also pass. All owned processes were waited and an
independent command/cwd scan found none remaining; state is retained.

Navigator uses Runtime 0.2.4 while the separate retrieval provider uses its
declared Runtime 0.2.2. Model/embedding responses and local identity bootstrap
are **SIMULATED**; source datasets and approved publication records are synthetic
seeds. Real external models/accounts, vendor delivery, deployment and remote
publication are **NOT_RUN**. This is an affected delta, not a relabeled rerun of
the earlier C1/G2 receipt: that full chain retains its Catalyst
`1593216baebcf82f1c3bc230a3b3e2392a20192d` provenance, including its 16-turn G2
and real-model NOT_RUN result. Its frozen state and receipt were not changed.

启动保护源码固定为上述 `0f4fb7c`，相邻 JSON 记录精确源码与证据摘要。
锁定 Runtime 0.2.0 环境的 129 项测试、Ruff、格式和 mypy 均通过；最初规范导出缺少
Git index 与 PATH 中的 python，导致 128 过、1 个环境失败，原负日志保留。
仅为导出夹具建立空本地 index、加入规范文件并使用独立 venv PATH 后，原用例及全套均通过；
未伪造上游提交历史、放宽断言或修改产品行为。

独立 Runtime 0.2.2 进程证据使用官方知识准备 Plugin 和 Artifact SDK：三个恢复标记
负例均为零 provider 调用、数据库/schema/nonce/文件字节不变；无标记时真实队列成功，
一次 Plugin 调用生成两个真实制品。第一次输入夹具缺 locator 的失败另行保留。

最终增量以新源码副本、新状态运行 Nav0db、Clientabd、Catalyst0f4，16 个浏览器门通过：
双用户真实目录、选包及检索正文隔离；实际修改并批准撤销来源后，在合成模型响应前阻止发送，
Task 失败且没有 active assistant。对话、记忆、人格、画像、图谱、附件隔离同样通过。
进程均已等待退出，独立进程扫描为零，合成状态保留。Nav 的 Runtime0.2.4 与检索端
声明的 Runtime0.2.2 使用独立环境。模型、embedding、身份引导为模拟；真实模型、账号、
平台投递、部署与远端发布未运行。原 Catalyst159 的完整 C1/G2 及其状态、回执保持原来源，
本次只证明受影响的最终增量，不将旧全套结果改名。
