package utils

import (
	"os"
	"strings"
	"testing"
)

func TestRandomString(t *testing.T) {
	t.Run("LengthCheck", func(t *testing.T) {
		lengths := []int{0, 1, 8, 32}
		for _, l := range lengths {
			got := RandomString(l)
			if len(got) != l {
				t.Errorf("RandomString(%d) length = %d; want %d", l, len(got), l)
			}
		}
	})

	t.Run("CharsetCheck", func(t *testing.T) {
		got := RandomString(100)
		for _, r := range got {
			if !strings.Contains(charset, string(r)) {
				t.Errorf("RandomString contains character %q not in charset", r)
			}
		}
	})

	t.Run("UniquenessCheck", func(t *testing.T) {
		// Statistically unlikely to be the same twice in a row for this length
		s1 := RandomString(16)
		s2 := RandomString(16)
		if s1 == s2 {
			t.Error("RandomString(16) returned identical strings consecutively")
		}
	})
}

func TestReplaceWithEnvVars(t *testing.T) {
	os.Setenv("SIPHON_TEST_VAR", "foo")
	os.Setenv("SIPHON_ANOTHER_VAR", "bar")
	defer func() {
		os.Unsetenv("SIPHON_TEST_VAR")
		os.Unsetenv("SIPHON_ANOTHER_VAR")
	}()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"NoMarkers", "plain text", "plain text"},
		{"SingleReplacement", "prefix %%SIPHON_TEST_VAR%% suffix", "prefix foo suffix"},
		{"MultipleReplacements", "%%SIPHON_TEST_VAR%%%%SIPHON_ANOTHER_VAR%%", "foobar"},
		{"MissingVar", "val: %%NON_EXISTENT_VARIABLE_XYZ%%", "val: "},
		{"InvalidMarkerFormat", "%%123_INVALID%%", "%%123_INVALID%%"}, // Starts with number, regex should skip
		{"PartialMarker", "%%SIPHON_TEST_VAR", "%%SIPHON_TEST_VAR"},
		{"MarkerWithUnderscores", "%%SIPHON_TEST_VAR%%", "foo"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ReplaceWithEnvVars(tt.input)
			if got != tt.expected {
				t.Errorf("ReplaceWithEnvVars(%q) = %q; want %q", tt.input, got, tt.expected)
			}
		})
	}
}
