#!/bin/bash
# 用法：bash ~/Research/voice-cs-eval/run.sh          （先自检，再全量测试）
#       bash ~/Research/voice-cs-eval/run.sh check    （只自检）
#       bash ~/Research/voice-cs-eval/run.sh all 豆包  （只测某一家）
cd "$(dirname "$0")" || exit 1
if ! command -v python3 >/dev/null 2>&1; then echo "没找到 python3，请把这行报错发给 Claude"; exit 1; fi
if [ ! -x .venv/bin/python ]; then
  echo "第一次运行，正在准备环境（约1分钟）……"
  python3 -m venv .venv || { echo "创建环境失败，请把上面的报错发给 Claude"; exit 1; }
fi
.venv/bin/python -m pip install -q --disable-pip-version-check "websockets>=13" || { echo "安装依赖失败，请把上面的报错发给 Claude"; exit 1; }
MODE="${1:-all}"
echo "开始运行（模式：${MODE}）。运行期间电脑不会自动睡眠，可以去做别的事。"
# 运行测试时不走代理/VPN：三家都是国内服务，直连才能测出真实延迟
env -u ALL_PROXY -u all_proxy -u HTTPS_PROXY -u https_proxy -u HTTP_PROXY -u http_proxy caffeinate -i .venv/bin/python eval.py "$@"
