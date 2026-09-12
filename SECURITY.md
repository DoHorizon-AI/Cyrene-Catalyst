# Security policy / 安全政策

## Reporting a vulnerability / 报告漏洞

Please report suspected vulnerabilities privately through a GitHub Security
Advisory after this repository is made public, or through the DoHorizon security
contact provided by the repository maintainers. Do not open a public issue with
an unpatched vulnerability, credentials, tenant data, or a full exploit.

仓库公开后，请通过 GitHub Security Advisory 私下报告疑似漏洞，或通过仓库
维护者提供的 DoHorizon 安全联系方式报告。不要在公开 Issue 中披露未修复漏洞、
凭证、租户数据或完整利用代码。

Include the affected commit or package version, deployment mode, operating
system, a minimal reproduction, and the expected and observed behavior. Redact
all secrets and personal data before sending the report.

报告请包含受影响的提交或包版本、部署模式、操作系统、最小复现步骤、预期行为
与实际行为。发送前请删除所有秘密和个人数据。

## Supported versions / 支持版本

This project is pre-1.0. Security fixes are developed on the default `main`
branch and are best-effort for released tags. There is no promise of a security
support window until a release policy is published.

本项目尚未达到 1.0。安全修复以默认 `main` 分支为主，并尽力回溯到已发布标签。
在正式发布支持政策前，不承诺固定的安全支持周期。

## Scope and limits / 范围与限制

Catalyst owns dataset lifecycle state, preparation intent, lineage, publication,
and direct Product/Plugin handoffs. Authentication, authorization, rate limits,
request-size limits, and network isolation belong to the hosting boundary unless
the API contract explicitly says otherwise. The local Artifact Plane adapter and
the `dataset.preparation.v1` provider must not be treated as a hostile-code or
multi-tenant sandbox.

Catalyst 负责数据集生命周期状态、整理意图、血缘、发布以及 Product/Plugin 直连
交接。除非 API 契约明确规定，认证、授权、限流、请求大小限制与网络隔离由宿主
边界负责。本地制品平面适配器与 `dataset.preparation.v1` 提供方不得被宣传为恶意
代码隔离环境或多租户安全边界。

The repository contains synthetic sample data only. The historical dialogue
corpus is not granted for redistribution by this repository.

本仓库只包含合成样例数据。历史对话语料不因本仓库而获得再分发许可。
