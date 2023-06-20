package main

import (
	"github.com/prometheus/common/model"
	"github.com/tidwall/gjson"
	"fmt"
)

func parser_json(text string) model.LabelSet {
	labels:=model.LabelSet{}
	if !gjson.Valid(text) {
		return labels
	}
	paths := get_paths(text,"",labels)
	for i := 0; i < len(paths); i++ {
		path:=paths[i]
		value :=gjson.Get(text,path)
		value_label :=value.Str
		if value.Type==gjson.Number{
			value_label=value.Raw
		}
		if value.Type==gjson.Null{
			value_label=""
		}
		label:=model.LabelSet{
			model.LabelName(path): model.LabelValue(value_label),
		}
		labels=labels.Merge(label)
	}
	return labels
}

func get_paths(json string, parent_path string, labels model.LabelSet) []string {
	var paths []string
	if !gjson.Valid(json) {
		return paths
	}
	parent := gjson.Parse(json)
	parent.ForEach(func(key, value gjson.Result) bool {
		new_path:=""
		if len(parent_path)==0{
			new_path=key.Str
		}else{
			if key.Type==gjson.Number{
				key_string:=fmt.Sprint(int(key.Num))
				new_path=parent_path+"."+key_string
			}
			if key.Type==gjson.String{
				new_path=parent_path+"."+key.Str
			}
		}
		
		if value.Type!=gjson.JSON{
			paths = append(paths, new_path)
			// value_label :=value.Str
			// if value.Type==gjson.Number{
			// 	value_label=value.Raw
			// }
			// if value.Type==gjson.Null{
			// 	value_label=""
			// }
			// label:=model.LabelSet{
			// 	model.LabelName(new_path): model.LabelValue(value_label),
			// }
			// labels=labels.Merge(label)
		}else{
			new_paths:=get_paths(value.Raw,new_path,labels)
			paths = append(paths, new_paths...)
		}
		return true // keep iterating
	})
	return paths
}