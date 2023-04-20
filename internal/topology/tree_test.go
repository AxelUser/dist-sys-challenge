package topology

import (
	"reflect"
	"testing"
)

func TestTree(t *testing.T) {
	type args struct {
		nodes    []string
		children int
	}
	tests := []struct {
		name string
		args args
		want map[string][]string
	}{
		{
			name: "8 nodes to 3-tree",
			args: args{
				nodes:    []string{"0", "1", "2", "3", "4", "5", "6", "7"},
				children: 3,
			},
			want: map[string][]string{
				"0": {"1", "2", "3"},
				"1": {"0", "4", "5", "6"},
				"2": {"0", "7"},
				"3": {"0"},
				"4": {"1"},
				"5": {"1"},
				"6": {"1"},
				"7": {"2"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Tree(tt.args.nodes, tt.args.children); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Tree() = %v, want %v", got, tt.want)
			}
		})
	}
}
