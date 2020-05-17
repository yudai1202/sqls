package parseutil

import (
	"fmt"

	"github.com/lighttiger2505/sqls/ast"
	"github.com/lighttiger2505/sqls/ast/astutil"
)

func ExtractSelectExpr(parsed ast.TokenList) []ast.Node {
	prefixMatcher := astutil.NodeMatcher{
		ExpectKeyword: []string{
			"SELECT",
			"ALL",
			"DISTINCT",
		},
	}
	peekMatcher := astutil.NodeMatcher{
		NodeTypes: []ast.NodeType{
			ast.TypeIdentiferList,
			ast.TypeIdentifer,
			ast.TypeMemberIdentifer,
			ast.TypeOperator,
			ast.TypeAliased,
			ast.TypeParenthesis,
			ast.TypeFunctionLiteral,
		},
	}
	return filterPrefixGroup(astutil.NewNodeReader(parsed), prefixMatcher, peekMatcher)
}

func ExtractTableReferences(parsed ast.TokenList) []ast.Node {
	prefixMatcher := astutil.NodeMatcher{
		ExpectKeyword: []string{
			"FROM",
			"UPDATE",
		},
	}
	peekMatcher := astutil.NodeMatcher{
		NodeTypes: []ast.NodeType{
			ast.TypeIdentiferList,
			ast.TypeIdentifer,
			ast.TypeMemberIdentifer,
			ast.TypeAliased,
		},
	}
	return filterPrefixGroupOnce(astutil.NewNodeReader(parsed), prefixMatcher, peekMatcher)
}

func ExtractTableReference(parsed ast.TokenList) []ast.Node {
	prefixMatcher := astutil.NodeMatcher{
		ExpectKeyword: []string{
			"INSERT INTO",
			"DELETE FROM",
		},
	}
	peekMatcher := astutil.NodeMatcher{
		NodeTypes: []ast.NodeType{
			ast.TypeIdentifer,
			ast.TypeMemberIdentifer,
			ast.TypeAliased,
		},
	}
	return filterPrefixGroup(astutil.NewNodeReader(parsed), prefixMatcher, peekMatcher)
}

func ExtractTableFactor(parsed ast.TokenList) []ast.Node {
	prefixMatcher := astutil.NodeMatcher{
		ExpectKeyword: []string{
			"JOIN",
		},
	}
	peekMatcher := astutil.NodeMatcher{
		NodeTypes: []ast.NodeType{
			ast.TypeIdentifer,
			ast.TypeMemberIdentifer,
			ast.TypeAliased,
		},
	}
	return filterPrefixGroup(astutil.NewNodeReader(parsed), prefixMatcher, peekMatcher)
}

func ExtractWhereCondition(parsed ast.TokenList) []ast.Node {
	prefixMatcher := astutil.NodeMatcher{
		ExpectKeyword: []string{
			"WHERE",
		},
	}
	peekMatcher := astutil.NodeMatcher{
		NodeTypes: []ast.NodeType{
			ast.TypeComparison,
			ast.TypeIdentiferList,
		},
	}
	return filterPrefixGroup(astutil.NewNodeReader(parsed), prefixMatcher, peekMatcher)
}

func ExtractAliasedIdentifer(parsed ast.TokenList) []ast.Node {
	reader := astutil.NewNodeReader(parsed)
	matcher := astutil.NodeMatcher{NodeTypes: []ast.NodeType{ast.TypeAliased}}
	aliases := reader.FindRecursive(matcher)

	results := []ast.Node{}
	for _, node := range aliases {
		alias, ok := node.(*ast.Aliased)
		if !ok {
			continue
		}
		fmt.Println("parse alias")
		list, ok := alias.RealName.(ast.TokenList)
		if !ok {
			results = append(results, node)
			continue
		}
		fmt.Println("parse list alias real name")
		if isSubQuery(list) {
			continue
		}
		fmt.Println("check sub query")
		results = append(results, node)
	}
	return results
}

func filterPrefixGroup(reader *astutil.NodeReader, prefixMatcher astutil.NodeMatcher, peekMatcher astutil.NodeMatcher) []ast.Node {
	var results []ast.Node
	for reader.NextNode(false) {
		if reader.CurNodeIs(prefixMatcher) && reader.PeekNodeIs(true, peekMatcher) {
			_, node := reader.PeekNode(true)
			results = append(results, node)
		}
		if list, ok := reader.CurNode.(ast.TokenList); ok {
			newReader := astutil.NewNodeReader(list)
			results = append(results, filterPrefixGroup(newReader, prefixMatcher, peekMatcher)...)
		}
	}
	return results
}

func filterPrefixGroupOnce(reader *astutil.NodeReader, prefixMatcher astutil.NodeMatcher, peekMatcher astutil.NodeMatcher) []ast.Node {
	results := filterPrefixGroup(reader, prefixMatcher, peekMatcher)
	if len(results) > 0 {
		return []ast.Node{results[0]}
	}
	return nil
}
