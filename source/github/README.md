# github

This driver is catered for those that want to source migrations from [github.com](https://github.com). The URL scheme doesn't require a hostname, as it just simply defaults to `github.com`.

`github://user:personal-access-token@owner/repo/path#ref`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| user | | The username of the user connecting |
| personal-access-token | | An access token from Github (https://github.com/settings/tokens) |
| owner | | the repo owner |
| repo | | the name of the repository |
| path | | path in repo to migrations |
| ref | | (optional) can be a SHA, branch, or tag |
