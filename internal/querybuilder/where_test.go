package querybuilder

import "testing"

type (
	customString   string
	customStringer struct{ value string }
)

func (c customStringer) String() string { return c.value }

func Test_SimpleWhere_Clause(t *testing.T) {
	value := "value"
	aliasValue := customString("value")
	var nilString *string
	var nilCustom *customString
	var nilStringer *customStringer

	tests := []struct {
		name  string
		where Where
		want  string
	}{
		{
			name:  "String",
			where: WhereEquals("name", "mark"),
			want:  "`name` = 'mark'",
		},
		{
			name:  "Numeric",
			where: WhereEquals("age", 3),
			want:  "`age` = 3",
		},
		{
			name:  "String alias",
			where: WhereEquals("name", customString("mark")),
			want:  "`name` = 'mark'",
		},
		{
			name:  "String pointer",
			where: WhereEquals("name", &value),
			want:  "`name` = 'value'",
		},
		{
			name:  "String pointer alias",
			where: WhereEquals("name", &aliasValue),
			want:  "`name` = 'value'",
		},
		{
			name:  "String fmt.Stringer",
			where: WhereEquals("name", customStringer{value: "value"}),
			want:  "`name` = 'value'",
		},
		{
			name:  "String fmt.Stringer pointer",
			where: WhereEquals("name", &customStringer{value: "value"}),
			want:  "`name` = 'value'",
		},
		{
			name:  "Typed nil pointer",
			where: WhereEquals("name", nilString),
			want:  "`name` IS NULL",
		},
		{
			name:  "Typed nil pointer alias",
			where: WhereEquals("name", nilCustom),
			want:  "`name` IS NULL",
		},
		{
			name:  "Typed nil pointer stringer",
			where: WhereEquals("name", nilStringer),
			want:  "`name` IS NULL",
		},
		{
			name:  "String with backtick in name",
			where: WhereEquals("te`st", "value"),
			want:  "`te\\`st` = 'value'",
		},
		{
			name:  "String Differs",
			where: WhereDiffers("name", "mark"),
			want:  "`name` <> 'mark'",
		},
		{
			name:  "Numeric Differs",
			where: WhereDiffers("age", 3),
			want:  "`age` <> 3",
		},
		{
			name:  "String Differs nil",
			where: WhereDiffers("name", nilString),
			want:  "`name` IS NOT NULL",
		},
		{
			name:  "String with backtick in name Differs",
			where: WhereDiffers("te`st", "value"),
			want:  "`te\\`st` <> 'value'",
		},
		{
			name:  "Null",
			where: IsNull("age"),
			want:  "`age` IS NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.where.Clause(); got != tt.want {
				t.Errorf("Clause() = %v, want %v", got, tt.want)
			}
		})
	}
}
