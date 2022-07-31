# github ee

## GitHub Enterprise Edition

This driver is catered for those who run GitHub Enterprise under private infrastructure.

The below URL scheme illustrates how to source migration files from GitHub Enterprise.

GitHub client for Go requires API and Uploads endpoint hosts in order to create an instance of GitHub Enterprise Client. We're making an assumption that the API and Uploads are available under `https://api.*` and `https://uploads.*` respectively. [GitHub Enterprise Installation Guide](https://help.github.com/en/enterprise/2.15/admin/installation/enabling-subdomain-isolation) recommends that you enable Subdomain isolation feature.

`github-ee://user:personal-access-token@host/owner/repo/path?verify-tls=true#ref`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| user | | The username of the user connecting |
| personal-access-token | | Personal access token from your GitHub Enterprise instance |
| owner | | the repo owner |
| repo | | the name of the repository |
| path | | path in repo to migrations |
| ref | | (optional) can be a SHA, branch, or tag |
| verify-tls | | (optional) defaults to `true`. This option sets `tls.Config.InsecureSkipVerify` accordingly |
