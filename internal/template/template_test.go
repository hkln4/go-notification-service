package template

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRender(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		vars        map[string]string
		want        string
		wantMissing []string
	}{
		{"plain", "Hello", nil, "Hello", nil},
		{"one", "Hi {{name}}!", map[string]string{"name": "Ayşe"}, "Hi Ayşe!", nil},
		{"spaces inside braces", "Hi {{ name }}", map[string]string{"name": "Ali"}, "Hi Ali", nil},
		{"repeated", "{{a}}-{{a}}", map[string]string{"a": "x"}, "x-x", nil},
		{"unused values ignored", "{{a}}", map[string]string{"a": "1", "b": "2"}, "1", nil},
		{"missing reported once, sorted", "{{b}} {{a}} {{b}}", nil, "{{b}} {{a}} {{b}}", []string{"a", "b"}},
		{"values are not re-expanded", "{{a}}", map[string]string{"a": "{{b}}", "b": "no"}, "{{b}}", nil},
		{"not a placeholder", "{{ 1x }} {single}", nil, "{{ 1x }} {single}", nil},
		{"empty value", "[{{a}}]", map[string]string{"a": ""}, "[]", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, missing := Render(tt.body, tt.vars)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantMissing, missing)
		})
	}
}

func TestVariables(t *testing.T) {
	require.Equal(t, []string{"name", "code"}, Variables("Hi {{name}}, code {{ code }} for {{name}}"))
	require.Empty(t, Variables("no placeholders"))
}

func TestValidName(t *testing.T) {
	for name, want := range map[string]bool{
		"order-shipped": true, "otp_v2": true, "": false, "Has Upper": false, "spaces here": false, "ü": false,
	} {
		require.Equal(t, want, ValidName(name), name)
	}
}
