#!/bin/bash
set -e

echo "🚀 Setting up pre-commit hooks..."

# Install pre-commit hooks
pre-commit install

echo "✅ Pre-commit hooks installed!"
echo ""
echo "ℹ️  CloudFormation files will be validated and linted on each commit."
echo "ℹ️  To manually run all hooks: pre-commit run --all-files"
echo "ℹ️  To manually run cfn-lint: cfn-lint cicd/**/*.yaml"
echo "ℹ️  To manually run cfn-nag: cfn_nag_scan --input-path cicd/"
