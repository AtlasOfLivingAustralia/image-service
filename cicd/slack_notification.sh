#!/bin/bash

####
# Send the deploy notification to Slack
#
# variables must be set as environment variables in the pipeline 

# Some fun, a different image for each environment
case $ENVIRONMENT in
  "production")
    IMAGE_URL=https://inaturalist-open-data.s3.amazonaws.com/photos/69895591/small.jpg
    ALT_TEXT="Photo by John D Reynolds. https://www.inaturalist.org/observations/44082017"
    ;;
  "staging")
    IMAGE_URL=https://inaturalist-open-data.s3.amazonaws.com/photos/208429788/small.jpg
    ALT_TEXT="Photo by Geoff Walker. https://www.inaturalist.org/observations/123058239"
    ;;
  "testing")
    IMAGE_URL=https://inaturalist-open-data.s3.amazonaws.com/photos/123146710/small.jpeg
    ALT_TEXT="Photo by Deb. https://www.inaturalist.org/observations/75285642"
    ;;
  *)
    IMAGE_URL=https://inaturalist-open-data.s3.amazonaws.com/photos/73679227/small.jpeg
    ALT_TEXT="Photo by Arthur Chapman. https://www.inaturalist.org/observations/46453228"
    ;;
esac

# if this is a PR inject a link
COMMIT_MSG=$(echo $COMMIT_MSG | sed "s~\(Merge pull request #\([0-9]*\)\)~<https://github.com/$REPO/pull/\2|\1>~g")

curl -X POST "https://slack.com/api/chat.postMessage" \
  -H "Content-type: application/json; charset=utf-8" \
  -H "Authorization: Bearer $SLACK_OAUTH_TOKEN" \
  -d @- << EOF
  {
    "channel": "$SLACK_ALERT_CHANNEL",
    "blocks": [
      {
        "type": "header",
        "text": {
          "type": "plain_text",
          "text": "$PRODUCT_NAME $PRODUCT_COMPONENT $ENVIRONMENT release",
          "emoji": true
        }
      },
      {
        "type": "section",
        "text": {
          "type": "mrkdwn",
          "text": "*Branch:*\n<https://github.com/$REPO/tree/$SRC_BRANCH|$SRC_BRANCH>\n*Environment:*\n$ENVIRONMENT\n"
        },
        "accessory": {
          "type": "image",
          "image_url": "$IMAGE_URL",
          "alt_text": "$ALT_TEXT"
        }
      },
      {
        "type": "section",
        "text": {
          "type": "mrkdwn",
          "text": "*Commit message:*\n$COMMIT_MSG - <https://github.com/$REPO/commit/$COMMIT_ID|$COMMIT_ID>"
        }
      },
      {
        "type": "section",
        "text": {
          "type": "mrkdwn",
          "text": "$DEPLOY_MSG"
        }
      }
   ]
  }
EOF
