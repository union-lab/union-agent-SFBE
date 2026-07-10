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
