// Package template renders message templates with {{variable}} placeholders.
//
// It is deliberately plain substitution rather than text/template: templates
// come in over the API, and a template language with functions, loops and
// conditionals is more power than a notification needs and harder to reason
// about. Values are inserted verbatim; nothing is evaluated.
package template

import (
	"regexp"
	"slices"
	"strings"
)

var placeholder = regexp.MustCompile(`\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}`)

// Variables returns the distinct placeholder names in body, in order of
// first appearance.
func Variables(body string) []string {
	var names []string
	for _, m := range placeholder.FindAllStringSubmatch(body, -1) {
		if !slices.Contains(names, m[1]) {
			names = append(names, m[1])
		}
	}
	return names
}

// Render replaces every placeholder with its value. It returns the names
// that have no value, sorted; the result is only meaningful when that list
// is empty. Unused values are ignored.
func Render(body string, vars map[string]string) (string, []string) {
	var missing []string
	out := placeholder.ReplaceAllStringFunc(body, func(m string) string {
		name := placeholder.FindStringSubmatch(m)[1]
		v, ok := vars[name]
		if !ok {
			if !slices.Contains(missing, name) {
				missing = append(missing, name)
			}
			return m
		}
		return v
	})
	slices.Sort(missing)
	return out, missing
}

// ValidName accepts 1-100 characters of lowercase letters, digits, '-' and
// '_', so names are safe in URLs and logs.
func ValidName(name string) bool {
	if name == "" || len(name) > 100 {
		return false
	}
	return strings.IndexFunc(name, func(r rune) bool {
		allowed := r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_'
		return !allowed
	}) < 0
}
