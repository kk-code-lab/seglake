package lifecycle

import (
	"strings"
	"testing"
	"time"
)

func TestRuleMatchesPrefixTagAndFilters(t *testing.T) {
	prefix := "logs/"
	tag := Tag{Key: "env", Value: "prod"}
	andPrefix := "logs/app/"
	rules := []struct {
		name string
		rule Rule
		key  string
		tags ObjectTags
		want bool
	}{
		{
			name: "legacy prefix",
			rule: Rule{Status: "Enabled", Prefix: &prefix},
			key:  "logs/a.txt",
			want: true,
		},
		{
			name: "filter tag",
			rule: Rule{Status: "Enabled", Filter: &Filter{Tag: &tag}},
			key:  "a.txt",
			tags: ObjectTags{{Key: "env", Value: "prod"}},
			want: true,
		},
		{
			name: "and prefix and tags",
			rule: Rule{Status: "Enabled", Filter: &Filter{And: &AndFilter{Prefix: &andPrefix, Tags: []Tag{{Key: "env", Value: "prod"}, {Key: "team", Value: "core"}}}}},
			key:  "logs/app/a.txt",
			tags: ObjectTags{{Key: "team", Value: "core"}, {Key: "env", Value: "prod"}},
			want: true,
		},
		{
			name: "and missing tag",
			rule: Rule{Status: "Enabled", Filter: &Filter{And: &AndFilter{Prefix: &andPrefix, Tags: []Tag{{Key: "env", Value: "prod"}, {Key: "team", Value: "core"}}}}},
			key:  "logs/app/a.txt",
			tags: ObjectTags{{Key: "env", Value: "prod"}},
			want: false,
		},
	}
	for _, tc := range rules {
		t.Run(tc.name, func(t *testing.T) {
			if got := RuleMatches(tc.rule, tc.key, tc.tags); got != tc.want {
				t.Fatalf("RuleMatches()=%v want %v", got, tc.want)
			}
		})
	}
}

func TestLifecycleEligibility(t *testing.T) {
	asOf := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	objectTime := asOf.AddDate(0, 0, -31)
	days := 30
	if !ExpirationEligible(Expiration{Days: &days}, objectTime, asOf) {
		t.Fatalf("expected day-based expiration to be eligible")
	}
	date := "2026-05-28"
	if !ExpirationEligible(Expiration{Date: date}, objectTime, asOf) {
		t.Fatalf("expected date-based expiration to be eligible")
	}
	noncurrentDays := 10
	if !NoncurrentEligible(NoncurrentExpiration{NoncurrentDays: &noncurrentDays}, asOf.AddDate(0, 0, -10), asOf) {
		t.Fatalf("expected noncurrent version to be eligible")
	}
	abortDays := 7
	if !MPUAbortEligible(AbortIncompleteMultipart{DaysAfterInitiation: &abortDays}, asOf.AddDate(0, 0, -8), asOf) {
		t.Fatalf("expected MPU abort to be eligible")
	}
}

func TestParseXMLRejectsDisabledOnlyNoCandidateRuleShape(t *testing.T) {
	body := `<LifecycleConfiguration><Rule><ID>off</ID><Status>Disabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`
	parsed, err := ParseXML(strings.NewReader(body))
	if err != nil {
		t.Fatalf("ParseXML: %v", err)
	}
	if len(parsed.NormalizedConfig.Rules) != 1 || RuleEnabled(parsed.NormalizedConfig.Rules[0]) {
		t.Fatalf("expected one disabled normalized rule")
	}
}
