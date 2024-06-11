package ydb

import (
	"context"
	"fmt"
	"net/url"

	"github.com/golang-jwt/jwt/v4"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

func withCredentials(q url.Values) ydb.Option {
	mode := q.Get("x-auth-mode")

	switch mode {
	case "":
		return ydb.With() // no-op

	case "anonymous":
		return ydb.WithAnonymousCredentials()

	case "static":
		return ydb.WithStaticCredentials(
			q.Get("x-auth-username"),
			q.Get("x-auth-password"),
		)

	case "access-token":
		return ydb.WithAccessTokenCredentials(
			q.Get("x-auth-access-token"),
		)

	case "oauth2":
		return withOauth2TokenExchangeCredentials(q)
	}

	return func(context.Context, *ydb.Driver) error {
		return fmt.Errorf("unknown x-auth-mode (%s)", mode)
	}
}

func withOauth2TokenExchangeCredentials(q url.Values) ydb.Option {
	var opts []credentials.Oauth2TokenExchangeCredentialsOption

	if v := q.Get("x-auth-token-endpoint"); v != "" {
		opts = append(opts, credentials.WithTokenEndpoint(v))
	}
	if v := q.Get("x-auth-grant-type"); v != "" {
		opts = append(opts, credentials.WithGrantType(v))
	}
	if v := q.Get("x-auth-resource"); v != "" {
		opts = append(opts, credentials.WithResource(v))
	}
	if v := q.Get("x-auth-requested-token-type"); v != "" {
		opts = append(opts, credentials.WithRequestedTokenType(v))
	}

	if v := q["x-auth-audience"]; len(v) > 0 {
		opts = append(opts, credentials.WithAudience(v...))
	}
	if v := q["x-auth-scope"]; len(v) > 0 {
		opts = append(opts, credentials.WithScope(v...))
	}

	token, err := subjectToken(q)
	if err != nil {
		return func(context.Context, *ydb.Driver) error {
			return err
		}
	}

	opts = append(opts, credentials.WithSubjectToken(token))
	return ydb.WithOauth2TokenExchangeCredentials(opts...)
}

func subjectToken(q url.Values) (credentials.TokenSource, error) {
	source := q.Get("x-auth-subject-token-source")

	switch source {
	case "fixed":
		return credentials.NewFixedTokenSource(
			q.Get("x-auth-subject-token"),
			q.Get("x-auth-subject-token-type"),
		), nil

	case "jwt":
		return credentials.NewJWTTokenSource(
			credentials.WithAudience(q["x-auth-subject-jwt-aud"]...),
			credentials.WithIssuer(q.Get("x-auth-subject-jwt-iss")),
			credentials.WithSubject(q.Get("x-auth-subject-jwt-sub")),
			credentials.WithID(q.Get("x-auth-subject-jwt-jti")),
			credentials.WithSigningMethod(jwt.GetSigningMethod(q.Get("x-auth-subject-jwt-alg"))),
			credentials.WithKeyID(q.Get("x-auth-subject-jwt-kid")),
			credentials.WithRSAPrivateKeyPEMFile(q.Get("x-auth-subject-jwt-pem-file")),
		)
	}

	return nil, fmt.Errorf("unknown x-auth-subject-token-source (%s)", source)
}
