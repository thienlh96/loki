package main

import (
	"fmt"
	"strings"

	"github.com/prometheus/common/model"
	"github.com/tidwall/gjson"
)

func parser_json(text string) model.LabelSet {
	labels := model.LabelSet{}
	region:= searchRegionFromIp(text)
	if region!=""{
		region_label:=model.LabelSet{
			model.LabelName("region"):model.LabelValue(region),
		}
		labels = labels.Merge(region_label)
	}

	if !gjson.Valid(text) {
		return labels
	}
	
	paths := get_paths(text, "", labels)
	for i := 0; i < len(paths); i++ {
		path := paths[i]
		value := gjson.Get(text, path)
		value_label := value.Str
		if value.Type == gjson.Number {
			value_label = value.Raw
		}
		if value.Type == gjson.Null {
			value_label = ""
		}
		if strings.Contains(path, "httpRequest.headers") && strings.Contains(path, "name") {
			continue
		}
		if strings.Contains(path, "httpRequest.headers") && strings.Contains(path, "value") {
			new_path := strings.Replace(path, "value", "name",1)
			path = "httpRequest.headers." + gjson.Get(text, new_path).Str
		}
		if len(value_label)>6048 {
			continue
		}
		if len(path)>1024 {
			continue
		}
		path=validPath(path)
		if (strings.Contains(path,"ruleGroupList")||strings.EqualFold(path,"httpRequest__headers__Accept")||strings.Contains(path,"httpRequest__headers__accept_encoding")||strings.Contains(path,"httpRequest__headers__accept_language")||strings.Contains(path,"httpRequest__headers__cache_control")||strings.Contains(path,"httpRequest__headers__authorization")){
			continue
		}
		if (strings.Contains(path,"httpRequest__headers__If_Modified_Since")||strings.Contains(path,"httpRequest__requestId")){
			continue
		}
		label := model.LabelSet{
			model.LabelName((path)): model.LabelValue(value_label),
		}
		labels = labels.Merge(label)
	}
	return labels
}

func validPath(path string) string{
	path=strings.ReplaceAll(path,"-","_")
	path=strings.ReplaceAll(path,".","__")
	return path
}

func get_paths(json string, parent_path string, labels model.LabelSet) []string {
	var paths []string
	if !gjson.Valid(json) {
		return paths
	}
	parent := gjson.Parse(json)
	parent.ForEach(func(key, value gjson.Result) bool {
		new_path := ""
		if len(parent_path) == 0 {
			new_path = key.Str
		} else {
			if key.Type == gjson.Number {
				key_string := fmt.Sprint(int(key.Num))
				new_path = parent_path + "." + key_string
			}
			if key.Type == gjson.String {
				new_path = parent_path + "." + key.Str
			}
		}

		if value.Type != gjson.JSON {
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
		} else {
			new_paths := get_paths(value.Raw, new_path, labels)
			paths = append(paths, new_paths...)
		}
		return true // keep iterating
	})
	return paths
}