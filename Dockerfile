# 参赛插件（AML · Agent Memory Leaderboard）容器
#
# 对应官方「学术代码提交」路线：给仓库 + Docker 入口，由平台部署并跑 smoke/正式评测。
#
# ⚠️ 两个模型服务（嵌入 / 重排）**不在本镜像里**：
#    它们默认指向 http://127.0.0.1:18099 / 18098。
#    容器内 127.0.0.1 指的是容器自己 —— 所以部署时**必须**用
#    `-e AML_EMBED_BASE=... -e AML_RERANK_BASE=...` 指向可达的地址，
#    或者把模型服务与 bot 合成一个 pod/网络命名空间。
#    `/health` 会如实报告这两个地址是否可达；不可达时检索退化为
#    词法 + 实体 + 时间三路（仍能工作，但语义路与 CE 重排缺席）。
#
# 构建：docker build -t shufang-aml .
# 运行：docker run -p 8080:8080 -e AML_API_KEY=xxx \
#         -e AML_EMBED_BASE=http://host.docker.internal:18099 \
#         -e AML_RERANK_BASE=http://host.docker.internal:18098 \
#         -v shufang-aml-data:/data shufang-aml

FROM python:3.12-slim

# jieba 首次分词要建前缀词典缓存（约 0.5s）；固化在镜像里省掉冷启动抖动
ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    TMPDIR=/tmp \
    SHUFANG_DATA_DIR=/data \
    AML_HOST=0.0.0.0 \
    AML_PORT=8080

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 预热 jieba 词典（把"第一次请求慢 0.5 秒"挪到构建期）
RUN python -c "import jieba; jieba.initialize()" || true

# 自检进构建期：契约断言不过就不该产出镜像
RUN python selftest.py || (echo "自检失败，镜像不产出" && exit 1)

VOLUME ["/data"]
EXPOSE 8080

# 用 exec 形式，让 SIGTERM 直达 python（否则容器停不下来要等 timeout）
ENTRYPOINT ["python", "-m", "services.C2_aml_service.server"]
