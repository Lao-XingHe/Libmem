import os, yaml, sys


def _eprint(*args, **kwargs):
    """诊断横幅一律走 stderr。

    stdio MCP 模式下 stdout 是 JSON-RPC 通道，导入期往 stdout 打字会插进协议流。
    """
    kwargs.setdefault("file", sys.stderr)
    print(*args, **kwargs)

_CONFIG = None

def _get_base_dir():
    env_dir = os.environ.get('SHUFANG_DATA_DIR')
    if env_dir:
        return env_dir
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    # 向上查找第一个包含 config.yaml 的目录（比硬编码层数更稳）
    here = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.exists(os.path.join(here, "config.yaml")):
            return here
        parent = os.path.dirname(here)
        if parent == here:
            # 没找到，回退到原逻辑（上 2 层）
            return os.path.dirname(os.path.dirname(here))
        here = parent

_BASE_DIR = _get_base_dir()
_CONFIG_PATH = os.path.join(_BASE_DIR, "config.yaml")
_PERSONAS_PATH = os.path.join(_BASE_DIR, "personas.yaml")
_PERSONAS = None
_eprint(f"[loader] SHUFANG_DATA_DIR={os.environ.get('SHUFANG_DATA_DIR', '未设置')}")
_eprint(f"[loader] BASE_DIR={_BASE_DIR}")
_eprint(f"[loader] CONFIG_PATH={_CONFIG_PATH}")
_eprint(f"[loader] PERSONAS_PATH={_PERSONAS_PATH}")

_DEFAULT_CONFIG = {
    "server": {"api_host": "127.0.0.1", "api_port": 8766, "indexer_knowledge_port": 8933, "indexer_memory_port": 8932},
    "llm": {
        "large": {"provider": "deepseek", "model": "deepseek-chat", "api_key": "", "base_url": "https://api.deepseek.com", "timeout": 120, "max_retries": 1},
        "small": {"backend": "llama_server", "model": "Qwen3.5-4B-Q4_K_M", "base_url": "http://127.0.0.1:8089/v1", "timeout": 120, "max_retries": 2, "model_path": "", "n_ctx": 4096, "n_gpu_layers": -1, "num_ctx": 131072, "enable_thinking": False},
        "local_engine": "llama_server",
    },
    "paths": {"data_root": "./data", "cold_vault": "./data/cold_vault", "warm_dir": "./data/warm", "index_dir": "./data/index", "knowledge_dir": "./data/knowledge"},
    "memory": {"hot_threshold": 5, "warm_threshold": 3, "keyword_temperature": 0.1},
    "extract_schedule": {"enabled": True, "mode": "interval", "interval": {"hours": 3}, "cron": {"times": ["08:00", "18:00", "22:00"]}},
    "knowledge": {"enabled": True, "max_file_size_mb": 50, "supported_extensions": [".txt", ".md", ".docx", ".pdf"]},
    "providers": {
        "deepseek": {"name": "DeepSeek", "base_url": "https://api.deepseek.com", "models": ["deepseek-chat", "deepseek-reasoner"]},
        "qwen": {"name": "通义千问", "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1", "models": ["qwen-max", "qwen-plus", "qwen-turbo"]},
        "glm": {"name": "智谱 GLM", "base_url": "https://open.bigmodel.cn/api/paas/v4", "models": ["glm-4", "glm-4-flash", "glm-4-plus"]},
        "kimi": {"name": "Kimi (月之暗面)", "base_url": "https://api.moonshot.cn/v1", "models": ["moonshot-v1-8k", "moonshot-v1-32k", "moonshot-v1-128k"]},
    },
}

_DEFAULT_PERSONAS = {
    "default_persona": "shufang_zhushou",
    "memory_enhance": True,
    "presets": {
        "shufang_zhushou": {"name": "书房助手", "description": "温文尔雅的私人书房助手", "prompt": "你是一位温文尔雅的书房助手。你为书房的主人服务，回答时多用'依我看来''不妨思考''细细想来'这类含蓄表达。你知识渊博但不炫耀，擅长用浅显比喻解释复杂概念。偶尔引用一两句诗词，但不过度。你的语气温和、耐心、言之有物，像一位得力的私人助手。"},
        "academic": {"name": "学术顾问", "description": "严谨的学术研究助手，注重逻辑与引用", "prompt": "你是一位严谨的学术研究顾问。回答问题时注重逻辑推导和事实依据，优先给出可查证的来源。你的表达精确、克制，避免情绪化和模糊表述。当不确定时，你会明确指出'这一点目前尚无定论'或'需要进一步验证'。你的回答结构清晰，善用分点论述。"},
        "code_master": {"name": "代码导师", "description": "经验丰富的编程导师，讲解清晰务实", "prompt": "你是一位经验丰富的编程导师。你的回答务实、清晰，每一段代码都附带简明解释。你看重最佳实践和工程思维，会指出'为什么这样做'以及'常见陷阱是什么'。你的风格直接但不失耐心，用通俗类比帮助理解底层原理。遇到问题你会先给出最小可行方案，再讨论优化方向。"},
        "creative": {"name": "创意伙伴", "description": "充满灵感的创意搭档，擅长头脑风暴", "prompt": "你是一位充满灵感的创意伙伴。你的思维跳跃但清晰，擅长从意想不到的角度切入问题。你热爱头脑风暴，会给出多个备选方案并分析各自的优劣。你的表达活泼、有画面感，偶尔使用'想象一下''换个角度看'等引导语。你鼓励用户突破惯性思维，但也会在关键处提醒可行性。"},
    },
    "custom_personas": [],
}

def load_config(config_path: str = None) -> dict:
    global _CONFIG, _CONFIG_PATH
    if _CONFIG is not None: return _CONFIG
    if config_path is not None: _CONFIG_PATH = config_path
    os.makedirs(os.path.dirname(_CONFIG_PATH), exist_ok=True)
    if not os.path.exists(_CONFIG_PATH):
        example = os.path.join(os.path.dirname(_CONFIG_PATH), "config.example.yaml")
        if os.path.exists(example):
            import shutil
            shutil.copy(example, _CONFIG_PATH)
            _eprint(f"已从 config.example.yaml 创建配置文件: {_CONFIG_PATH}")
        else:
            with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
                yaml.dump(_DEFAULT_CONFIG, f, allow_unicode=True, default_flow_style=False)
            _eprint(f"已自动创建默认配置文件: {_CONFIG_PATH}")
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f: _CONFIG = yaml.safe_load(f)
    _resolve_paths(_CONFIG)
    if "personas" in _CONFIG:
        _migrate_personas(_CONFIG)
    return _CONFIG


def _migrate_personas(config: dict):
    old = config.pop("personas")
    if not os.path.exists(_PERSONAS_PATH):
        pd = _DEFAULT_PERSONAS.copy()
        pd["default_persona"] = old.get("default_persona", pd["default_persona"])
        pd["memory_enhance"] = old.get("memory_enhance", pd["memory_enhance"])
        if old.get("presets"):
            pd["presets"] = old["presets"]
        if old.get("custom_personas"):
            pd["custom_personas"] = old["custom_personas"]
        os.makedirs(os.path.dirname(_PERSONAS_PATH), exist_ok=True)
        with open(_PERSONAS_PATH, "w", encoding="utf-8") as f:
            yaml.dump(pd, f, allow_unicode=True, default_flow_style=False)
        _eprint(f"[loader] 人设已从 config.yaml 迁移到 personas.yaml")
        cfg_to_save = _config_for_save(config)
        with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
            yaml.dump(cfg_to_save, f, allow_unicode=True, default_flow_style=False)


def _resolve_paths(config: dict):
    base = os.path.dirname(_CONFIG_PATH)
    defaults = _DEFAULT_CONFIG.get("paths", {})
    for key in ("cold_vault", "warm_dir", "knowledge_dir", "index_dir", "data_root"):
        p = defaults.get(key, "./data")
        config["paths"][key] = os.path.normpath(os.path.join(base, p))
    for key in ("cold_vault", "warm_dir", "knowledge_dir", "index_dir"):
        p = config.get("paths", {}).get(key)
        if p: os.makedirs(p, exist_ok=True)
    _eprint(f"[loader] 数据路径已解析:")
    _eprint(f"[loader]   cold_vault={config.get('paths',{}).get('cold_vault')}")
    _eprint(f"[loader]   warm_dir={config.get('paths',{}).get('warm_dir')}")
    _eprint(f"[loader]   index_dir={config.get('paths',{}).get('index_dir')}")

def get_base_dir() -> str:
    """当前生效的 BASE_DIR（config.yaml 所在目录）。

    2026-09-29 加：`dimensions.py` 要在同一目录找 `dimensions.yaml`，
    而 `_BASE_DIR` 是私有的。评测侧靠 `SHUFANG_DATA_DIR` 把 BASE_DIR 指到
    `locomo-full/stage2`，所以这里**不能**用 `__file__` 去推 —— 必须读同一个来源，
    否则会出现"产品读一个维度定义、评测读另一个"的分叉（正是 v1.3.1 要消掉的那类问题）。
    """
    return _BASE_DIR


def get_config() -> dict:
    if _CONFIG is None: return load_config()
    return _CONFIG

def get_data_path(key: str, default: str = "./data") -> str:
    config = get_config()
    return config.get("paths", {}).get(key, default)

def _config_for_save(config: dict) -> dict:
    cfg_to_save = {}
    for k, v in config.items():
        if k == "paths":
            cfg_to_save[k] = {}
            base = os.path.dirname(_CONFIG_PATH)
            for pk, pv in v.items():
                if isinstance(pv, str) and os.path.isabs(pv):
                    try:
                        cfg_to_save[k][pk] = os.path.relpath(pv, base)
                    except ValueError:
                        cfg_to_save[k][pk] = pv
                else:
                    cfg_to_save[k][pk] = pv
        else:
            cfg_to_save[k] = v
    return cfg_to_save

def load_personas() -> dict:
    global _PERSONAS
    if _PERSONAS is not None:
        return _PERSONAS
    if not os.path.exists(_PERSONAS_PATH):
        _PERSONAS = _DEFAULT_PERSONAS.copy()
        os.makedirs(os.path.dirname(_PERSONAS_PATH), exist_ok=True)
        with open(_PERSONAS_PATH, "w", encoding="utf-8") as f:
            yaml.dump(_PERSONAS, f, allow_unicode=True, default_flow_style=False)
    else:
        with open(_PERSONAS_PATH, "r", encoding="utf-8") as f:
            _PERSONAS = yaml.safe_load(f) or _DEFAULT_PERSONAS.copy()
    return _PERSONAS

def get_personas() -> dict:
    return load_personas()

def save_personas(data: dict) -> dict:
    global _PERSONAS
    _PERSONAS = data
    os.makedirs(os.path.dirname(_PERSONAS_PATH), exist_ok=True)
    with open(_PERSONAS_PATH, "w", encoding="utf-8") as f:
        yaml.dump(_PERSONAS, f, allow_unicode=True, default_flow_style=False)
    return _PERSONAS

def save_config(config: dict) -> dict:
    global _CONFIG
    _CONFIG = config
    cfg_to_save = _config_for_save(config)
    with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.dump(cfg_to_save, f, allow_unicode=True, default_flow_style=False)
    return _CONFIG

def reload_config() -> dict:
    global _CONFIG, _PERSONAS
    _CONFIG = None
    _PERSONAS = None
    return load_config()

def reload_personas() -> dict:
    global _PERSONAS
    _PERSONAS = None
    return load_personas()
