#!/usr/bin/env bash
# Quant Data Pipeline Skill — 一键安装脚本
#
# 用法:
#   curl -fsSL https://raw.githubusercontent.com/ywbhnay/quant-data-pipeline-skill/main/install.sh | bash
#
# 或直接运行:
#   bash install.sh

set -euo pipefail

REPO="ywbhnay/quant-data-pipeline-skill"
BRANCH="main"
CLONE_DIR="$HOME/.local/share/quant-data-pipeline"
SYMLINK="$HOME/.claude/skills/quant-data-pipeline"

echo "=========================================="
echo "  Quant Data Pipeline Skill 安装程序"
echo "=========================================="
echo ""

# 1. Clone or pull latest
if [ -d "$CLONE_DIR/.git" ]; then
    echo "检测到已有安装，正在拉取最新代码..."
    (cd "$CLONE_DIR" && git pull origin "$BRANCH" --quiet)
else
    echo "正在克隆仓库..."
    rm -rf "$CLONE_DIR"
    git clone --quiet --branch "$BRANCH" "https://github.com/${REPO}.git" "$CLONE_DIR"
fi
echo "  已安装到: $CLONE_DIR"
echo ""

# 2. Install Python dependencies
echo "正在安装 Python 依赖..."
pip install -r "$CLONE_DIR/requirements.txt" --quiet
echo "  依赖安装完成"
echo ""

# 3. Create symlink to Claude skills directory
echo "正在注册 Claude Skill..."
mkdir -p "$(dirname "$SYMLINK")"
rm -f "$SYMLINK"
ln -s "$CLONE_DIR" "$SYMLINK"
echo "  已注册到: $SYMLINK"
echo ""

# 4. Check .env
if [ ! -f "$CLONE_DIR/.env" ]; then
    echo "检测到 .env 文件不存在，正在从模板创建..."
    cp "$CLONE_DIR/.env.example" "$CLONE_DIR/.env"
    echo "  已创建: $CLONE_DIR/.env"
    echo "  请在 .env 文件中填写 DB_* 和 TUSHARE_TOKEN 配置"
else
    echo "  .env 文件已存在"
fi
echo ""

echo "=========================================="
echo "  安装完成!"
echo "=========================================="
echo ""
echo "后续步骤:"
echo "1. 编辑 $CLONE_DIR/.env 填写数据库和 Tushare 配置"
echo "2. 在 Claude 中使用 manage_quant_data 工具即可调用"
echo ""
echo "快速验证:"
echo "  python $CLONE_DIR/quant_data_tool.py status"
echo ""
