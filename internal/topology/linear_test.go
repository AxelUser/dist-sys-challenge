package topology

import (
	"reflect"
	"testing"
)

func TestLinear(t *testing.T) {
	type args struct {
		nodes []string
	}
	tests := []struct {
		name string
		args args
		want map[string][]string
	}{
		{
			name: "8 nodes should form linear cycle",
			args: args{
				nodes: []string{"0", "1", "2", "3", "4", "5", "6", "7"},
			},
			want: map[string][]string{
				"0": {"1"},
				"1": {"2"},
				"2": {"3"},
				"3": {"4"},
				"4": {"5"},
				"5": {"6"},
				"6": {"7"},
				"7": {"0"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Linear(tt.args.nodes); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Linear() = %v, want %v", got, tt.want)
			}
		})
	}
}
