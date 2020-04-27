# gitlab

`gitlab://user:personal-access-token@gitlab_url/project_id/path#ref?per_page=20`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| user | | The username of the user connecting |
| personal-access-token | | An access token from Gitlab (https://<gitlab_url>/profile/personal_access_tokens) |
| gitlab_url | GitlabUrl | url of the gitlab server |
| project_id | ProjectID | id of the repository |
| path | Path | path in repo to migrations |
| ref | Ref | (optional) can be a SHA, branch, or tag |
| per_page | PerPage | (optional) [Offset-based pagination for Gitlab](https://gitlab.com/help/api/README.md#offset-based-pagination) |
