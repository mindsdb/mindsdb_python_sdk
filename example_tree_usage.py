#!/usr/bin/env python3
"""
Example usage of the new tree functionality in MindsDB Python SDK.

This demonstrates how to use the tree() methods to explore database structures.
"""

import mindsdb_sdk


def main():
    """Example usage of tree functionality."""
    
    # Connect to MindsDB (adjust URL as needed)
    server = mindsdb_sdk.connect('http://127.0.0.1:47334')
    
    print("=== MindsDB Tree Exploration Example ===\n")
    
    # Get the tree of all databases
    print("1. Getting databases tree:")
    try:
        databases_tree = server.tree()
        
        for db_node in databases_tree:
            print(f"  📁 Database: {db_node.name}")
            print(f"     Type: {db_node.type}")
            print(f"     Engine: {db_node.engine}")
            print(f"     Deletable: {db_node.deletable}")
            print(f"     Visible: {db_node.visible}")
            print()
            
    except Exception as e:
        print(f"Error getting databases tree: {e}")
        return
    
    # Explore a specific database
    print("2. Exploring a specific database:")
    if databases_tree:
        # Use the first database as an example
        example_db_name = databases_tree[0].name
        print(f"Exploring database: {example_db_name}")
        
        try:
            database = server.databases.get(example_db_name)
            
            # Get tables without schema info
            print(f"\n  Tables in {example_db_name} (basic):")
            basic_tree = database.tree(with_schemas=False)
            for item in basic_tree:
                print(f"    📋 {item.name} ({item.class_}) - Type: {item.type}")
            
            # Get tables with schema info (if applicable)
            print(f"\n  Tables in {example_db_name} (with schemas):")
            detailed_tree = database.tree(with_schemas=True)
            for item in detailed_tree:
                if item.class_ == 'schema':
                    print(f"    📁 Schema: {item.name}")
                    if item.children:
                        for child in item.children:
                            print(f"      📋 {child.name} ({child.type})")
                else:
                    print(f"    📋 {item.name} ({item.class_}) - Type: {item.type}")
                    if hasattr(item, 'schema') and item.schema:
                        print(f"       Schema: {item.schema}")
                        
        except Exception as e:
            print(f"Error exploring database {example_db_name}: {e}")
    
    print("\n=== Tree exploration complete ===")


if __name__ == "__main__":
    main()
