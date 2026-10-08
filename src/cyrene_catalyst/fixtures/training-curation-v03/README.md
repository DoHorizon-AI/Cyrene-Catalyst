# Fixed mixed business acceptance corpus / 固定混合业务验收语料

These bilingual business examples were authored for acceptance and contain no customer data. The four input files contain 28 records, including one malformed JSONL line and one blank line. `fixture-manifest.json` records original digests, counts, curation settings, review decisions, provenance, and license.

这些中英文业务示例专为验收编写，不包含客户数据，也未复制第三方数据集。四个输入文件共有 28 条记录（包括一条损坏的 JSONL 行和一条空行）。`fixture-manifest.json` 保存原件摘要、数量、清洗参数、审核决定、来源说明和许可证。

覆盖 Alpaca、Prompt-Completion、messages、ShareGPT、ChatML、多轮对话与 system、Unicode/换行、跨文件精确重复、来源族和共同对话血缘、缺失字段、错误类型/角色/顺序、空回答、超长内容、tool calls、多模态结构、自定义字段映射及明确禁止训练的记录。

验收通过产品 API 导入与审核，不直接编写转换逻辑。验收审核决定只适用于这组固定的作者编写样例。人工映射保留原字段及处理历史；不适合文本 SFT 的记录明确排除。最终数据包由独立标准库消费者读取，检查文件摘要、严格 schema、来源族/对话隔离及管理字段分离。真实训练器加载另行在固定版本的 CPU 环境执行，不启动训练。

The fixture is distributed with the Catalyst package under the repository's Apache-2.0 license. It is test content, not a customer dataset or a production training recommendation.

此夹具随 Catalyst 包按仓库 Apache-2.0 许可证分发。它是测试内容，不是客户数据集或生产训练建议。
