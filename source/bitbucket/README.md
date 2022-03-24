# bitbucket

This driver is catered for those that want to source migrations from bitbucket cloud(https://bitbucket.com).

`bitbucket://user:password@owner/repo/path#ref`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| user | | The username of the user connecting |
| password | | User's password or an app password with repo read permission  |
| owner | | the repo owner |
| repo | | the name of the repository |
| path | | path in repo to migrations |
| ref | | (optional) can be a SHA, branch, or tag |
