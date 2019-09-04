# gitlab

`gitlab://user:personal-access-token@gitlab_url/project_id/path#ref`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| user | | The username of the user connecting |
| personal-access-token | | An access token from Gitlab (https://<gitlab_url>/profile/personal_access_tokens) |
| gitlab_url | | url of the gitlab server |
| project_id | | id of the repository |
| path | | path in repo to migrations |
| ref | | (optional) can be a SHA, branch, or tag |
