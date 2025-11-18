#!/bin/bash
set -ueo pipefail

###
# Deploy the codepipeline for the images app
# You must have AWS CLI authentication for this to run.

usage() {
 echo "Usage: $0 [OPTIONS]"
 echo "Options:"
 echo " -e      The environment. Optional but must be \"prod\" to launch in production"
 echo " -b      Branch override. Used when we're not on a branch (detached head)"
}

ENV=nonprod
BRANCH_OVERRIDE=
SCRIPT_DIR=$(dirname "$(realpath "$0")")

while getopts "e:b:" flag; do
 case $flag in
   e) # Handle the -e environment flag
   # must be "prod" to launch in production
   ENV=$OPTARG
   ;;
   b) # Handle the -b branch override flag
   # branch override is used when we're not on a branch (detached head)
   BRANCH_OVERRIDE=$OPTARG
   ;;
   \?)
   usage
   exit 1
   ;;
 esac
done

# get the branch
branch=$(git branch --show-current)

# confirm which environment we're deploying to if it wasnt explicitly set
if [ "$branch" = "main" ] && [ "$ENV" = "nonprod" ]; then
  echo "Deploy to production or staging?"
  echo "1) production"
  echo "2) staging"
  read -p "Enter your choice (1 or 2): " choice

  case $choice in
    1)
      ENV="prod"
      ;;
  esac
fi

# check if we're on a detached head
if [[ -n $branch ]]; then
  real_branch=1
elif [[ -z $branch && -n $BRANCH_OVERRIDE ]]; then
  real_branch=0
  branch=$BRANCH_OVERRIDE
else
  echo "You must specify a branch override or run this script from a git branch"
  exit 1;
fi

echo branch: $branch

# get the commit_ids
COMMIT_ID=$(git rev-parse HEAD)
echo commit id: $COMMIT_ID
LATEST_COMMIT_ID=$(git log -n 1 --pretty=format:"%H" $branch)

# check if we're rolling back to a previous commit
if [[ "$COMMIT_ID" != "$LATEST_COMMIT_ID" ]]; then
  echo latest commit is $LATEST_COMMIT_ID
  echo but we are releasing $COMMIT_ID
  echo we are rolling back to a previous commit
  RESTART_PIPELINE_ON_UPDATE=false
else
  RESTART_PIPELINE_ON_UPDATE=true
fi

# check that any changes are commited and pushed
if [[ $real_branch -eq 1 && -n "$(git status --porcelain)" ]] ; then
  echo "changes must be committed and pushed before deploying"
  exit 1;
fi

# check the remote branch exists
if [[ $real_branch -eq 1 ]] && ! git ls-remote --exit-code origin $branch > /dev/null 2>&1 ; then
  echo "changes must be committed and pushed before deploying"
  exit 1;
fi

# check there are no differences with remote
if [[ $real_branch -eq 1 && -n "$(git diff $branch origin/$branch)" ]] ; then
  echo "changes must be committed and pushed before deploying"
  exit 1;
fi

# get the clean version of the branch
clean_branch=$($SCRIPT_DIR/../../clean_branch.sh $branch)
echo clean branch: $clean_branch

# get the environment based on the branch
environment=$($SCRIPT_DIR/../../branch_2_env.py --branch $branch --env $ENV)
echo environment: $environment

# load environment vars
$SCRIPT_DIR/../../gen_env_vars.py --env $environment  --clean-branch $clean_branch --conf $SCRIPT_DIR/../config.ini > env.txt
source env.txt
rm env.txt

# Determine the operating system
OS=$(uname)

# Calculate pipeline MD5 based on the operating system
case "$OS" in
    "Darwin")
        # macOS
        PIPELINE_MD5=$(md5 -q pipeline.yaml)
        ;;
    "Linux")
        # Linux
        PIPELINE_MD5=$(md5sum pipeline.yaml | awk '{ print $1 }')
        ;;
    *)
        echo "Unsupported OS: $OS"
        exit 1
        ;;
esac

# deploy/update the template
echo "Deploying the pipeline template"
aws cloudformation deploy \
    --template-file pipeline.yaml \
    --stack-name $PIPELINE_STACK_NAME \
    --tags product=$PRODUCT_NAME component=cicd environment=$environment branch=$branch version=$COMMIT_ID \
    --region $REGION \
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
    --parameter-overrides \
        pAutoDeploy=$AUTO_DEPLOY \
        pBootstrapStackName=$BOOTSTRAP_STACK_NAME \
        pBucketsStackName=$BUCKETS_STACK_NAME \
        pCleanBranch=$clean_branch \
        pEnvironment=$environment \
        pGitHubBranch=$branch \
        pGitHubOwner=$GITHUB_OWNER \
        pGitHubRepositoryName=$GITHUB_REPO_NAME \
        pPipelineFingerprint=$PIPELINE_MD5 \
        pProductComponent=$PRODUCT_COMPONENT \
        pProductName=$PRODUCT_NAME \
        pSecretName=$SECRET_NAME \
        pEksNamespace=$EKS_NAMESPACE \
        pRestartExecutionOnUpdate=$RESTART_PIPELINE_ON_UPDATE \

