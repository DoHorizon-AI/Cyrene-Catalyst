# Restored-state startup hold / 恢复状态启动暂停

Ordinary Catalyst app, CLI, SQLite Store and ProcessingRunCoordinator startup
rejects `RESTORE_SUSPENDED.json` on any lexical or resolved ancestor of the state,
artifact, working or source path. The check runs before directory creation,
SQLite connection/schema migration, worker creation or queued-run recovery.
Malformed files, directories and dangling markers are all holds; unreadable or
missing path authority fails closed. No marker contents are trusted or changed.

The processing store retains both its original absolute lexical path and its
original resolved path. The coordinator rechecks those authorities before
creating its executor, including when handed a store opened earlier. A caller
cannot omit storage authority to enable recovery.

Restored data must be inspected through a read-only projection of an isolated
copy. Ordinary Store constructors initialize schema and are not inspection
readers. This change adds no release, marker-removal or replay interface. Normal
unmarked startup keeps the existing queued-run behavior.

Tests cover actual OS-process startup rejection with canonical queued external
work, zero provider calls and unchanged database/nonce/schema bytes, as well as
unmarked recovery. A separate integration proof uses the actual official
knowledge preparation Plugin over gRPC and the Artifact SDK; source data and
approved input seeding are synthetic, with no external model/account calls.

普通 Catalyst app、CLI、SQLite Store 和 ProcessingRunCoordinator 启动时，会检查状态、
制品、工作目录和源码路径的词法及解析后祖先目录中的 `RESTORE_SUSPENDED.json`。
检查先于建目录、连接数据库、迁移 schema、创建工作线程及恢复旧队列。
畸形文件、目录、悬空标记都表示暂停；无法核实路径权限或缺少存储路径也会拒绝。
门禁不信任、修改或删除标记内容。

处理 Store 保存构造时的绝对词法路径和解析路径，协调器创建线程池前再次核实，
不会因为工作目录或路径别名变化而猜测另一处存储。恢复副本应通过隔离副本的只读投影
检查；普通 Store 会初始化 schema，不能充当恢复检查器。本改动不提供解除暂停、
删除标记或重放接口；无恢复标记的正常启动行为保持不变。
