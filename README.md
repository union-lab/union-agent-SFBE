# union-agent-SFBE

顺丰 WMS 独立后端服务，从 `union-agent` 拆出。

## API 边界

- `POST /api/sf/callback`
- `GET /api/sf/push-logs`
- `/api/sf/dashboard/*`
- `/api/sf/outbound/*`

第一阶段仍复用原 PostgreSQL 数据库，不拆表归属。生产建议由 nginx 按 `/api/sf/` 转发到本服务，例如：

```nginx
location /api/sf/ {
    proxy_pass http://127.0.0.1:8010/api/sf/;
}
```

公网只暴露 `80/443`，本服务端口只绑定 `127.0.0.1` 或 Docker 内网。

本服务的数据库连接池初始化不做 DDL/DML 启动修复，避免独立服务试运行时影响主系统共享库。

## 本地验证

```bash
uv sync
uv run python -c "from app.main import app; print(app.title)"
uv run uvicorn app.main:app --host 127.0.0.1 --port 8010
```

## 生产容器建议

```bash
docker build -t union-agent-sfbe:latest .
docker run -d \
  --name union-agent-sfbe \
  --restart always \
  --env-file /opt/union-agent-sfbe/.env.production \
  -p 127.0.0.1:8010:8010 \
  union-agent-sfbe:latest
```

## CI/CD

- `.github/workflows/ci.yml`：Pull Request 和 `main` 推送执行静态检查、测试与编译检查。
- `.github/workflows/deploy-production.yml`：业务代码推送到 `main` 后自动部署；也可手动触发。
- 普通代码变更复用当前生产依赖镜像并覆盖完整 `app`；依赖、锁文件或 Dockerfile 变化时全量构建。
- 部署脚本不执行数据库 migration 或 SQL；服务自身保持现有运行逻辑。新容器健康检查失败时自动恢复旧容器。
- 自动部署要求仓库配置 `CVM_HOST`、`CVM_USER`、`CVM_SSH_KEY`，企微通知可选配置 `DEPLOY_WECOM_WEBHOOK`。
