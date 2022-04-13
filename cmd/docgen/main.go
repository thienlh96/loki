package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/grafana/loki/pkg/loki"
)

const (
	maxLineWidth = 80
	tabWidth     = 2
)

func removeFlagPrefix(block *configBlock, prefixes []string) {
	for _, entry := range block.entries {
		switch entry.kind {
		case "block":
			// Skip root blocks
			if !entry.root {
				removeFlagPrefix(entry.block, prefixes)
			}
		case "field":
			for _, prefix := range prefixes {
				if prefix != "" && strings.HasPrefix(entry.fieldFlag, prefix) {
					entry.fieldFlag = "<prefix>" + entry.fieldFlag[len(prefix):]
				}
			}
		}
	}
}

func annotateFlagPrefix(blocks []*configBlock) {
	// Find duplicated blocks
	groups := map[string][]*configBlock{}
	for _, block := range blocks {
		groups[block.name] = append(groups[block.name], block)
	}

	// For each duplicated block, we need to fix the CLI flags, because
	// in the documentation each block will be displayed only once but
	// since they're duplicated they will have a different CLI flag
	// prefix, which we want to correctly document.
	for _, group := range groups {
		if len(group) == 1 {
			continue
		}

		// We need to find the CLI flags prefix of each config block. To do it,
		// we pick the first entry from each config block and then find the
		// different prefix across all of them.
		flags := []string{}
		for _, block := range group {
			for _, entry := range block.entries {
				if entry.kind == "field" {
					flags = append(flags, entry.fieldFlag)
					break
				}
			}
		}

		allPrefixes := []string{}
		for i, prefix := range findFlagsPrefix(flags) {
			group[i].flagsPrefix = prefix
			allPrefixes = append(allPrefixes, prefix)
		}

		// Store all found prefixes into each block so that when we generate the
		// markdown we also know which are all the prefixes for each root block.
		for _, block := range group {
			block.flagsPrefixes = allPrefixes
		}
	}

	// Finally, we can remove the CLI flags prefix from the blocks
	// which have one annotated.
	for _, block := range blocks {
		removeFlagPrefix(block, block.flagsPrefixes)
	}
}

func generateBlocksMarkdown(blocks []*configBlock) string {
	md := &markdownWriter{}
	md.writeConfigDoc(blocks)
	return md.string()
}

func main() {
	// Parse the generator flags.
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintf(os.Stderr, "Usage: doc-generator template-file")
		os.Exit(1)
	}

	templatePath := flag.Arg(0)

	// In order to match YAML config fields with CLI flags, we do map
	// the memory address of the CLI flag variables and match them with
	// the config struct fields address.
	cfg := &loki.Config{}
	flags := parseFlags(cfg)

	// Parse the config, mapping each config field with the related CLI flag.
	blocks, err := parseConfig(nil, cfg, flags)
	if err != nil {
		fmt.Fprintf(os.Stderr, "An error occurred while generating the doc: %s\n", err.Error())
		os.Exit(1)
	}

	// Annotate the flags prefix for each root block, and remove the
	// prefix wherever encountered in the config blocks.
	annotateFlagPrefix(blocks)

	// Generate documentation markdown.
	data := struct {
		ConfigFile           string
		GeneratedFileWarning string
	}{
		ConfigFile: generateBlocksMarkdown(blocks),
		GeneratedFileWarning: "<!-- DO NOT EDIT THIS FILE - This file has been automatically generated " +
			"from its .template -->",
	}
	// Load the template file.
	tpl := template.New(filepath.Base(templatePath))
	tpl, err = tpl.ParseFiles(templatePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "An error occurred while loading the template %s: %s\n", templatePath, err.Error())
		os.Exit(1)
	}

	// Execute the template to inject generated doc.
	if err := tpl.Execute(os.Stdout, data); err != nil {
		fmt.Fprintf(os.Stderr, "An error occurred while executing the template %s: %s\n", templatePath, err.Error())
		os.Exit(1)
	}
}