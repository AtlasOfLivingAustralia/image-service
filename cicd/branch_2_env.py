#!/usr/bin/env python3

###
# convert the current code branch to an environment, used to load the appropriate environment vars
# Usage: ./branch_2_env.py --branch [code-branch] --env [environment]

import argparse
import re

parser = argparse.ArgumentParser()

parser.add_argument("--branch", "-b", help="The code branch that triggered this build", required=True)
parser.add_argument("--env",    "-e", help="Environment", required=False, default='non-prod')

args = parser.parse_args()

# associate the branch with an environment 
if re.search('^main$|^master$', args.branch) and args.env == 'prod':
  #print(f"Branch {args.branch} matched main")
  print('production')
elif re.search('^main$|^master$', args.branch):
  #print(f"Branch {args.branch} matched release")
  print('staging')
elif re.search('^testing$', args.branch):
  #print(f"Branch {args.branch} matched testing")
  print('testing')
else:
  #print(f"Branch {args.branch} didnt match")
  print('development')
   
