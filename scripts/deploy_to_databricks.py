#!/usr/bin/env python3
"""
Deployment script for Databricks SCD Library

This script helps package the library for deployment to Databricks workspace.
"""

import os
import shutil
import zipfile
from pathlib import Path

def create_databricks_package():
    """Create a package suitable for Databricks deployment."""
    
    # Define paths
    project_root = Path(__file__).parent.parent
    src_path = project_root / "src" / "libraries" / "dimensional_processing"
    notebooks_path = project_root / "notebooks"
    output_path = project_root / "databricks_package"
    
    print("🚀 Creating Databricks deployment package...")
    
    # Create output directory
    if output_path.exists():
        shutil.rmtree(output_path)
    output_path.mkdir()
    
    # Copy library source code
    lib_dest = output_path / "dimensional_processing"
    shutil.copytree(src_path, lib_dest)
    print(f"✅ Copied library source to {lib_dest}")
    
    # Copy notebooks
    notebooks_dest = output_path / "notebooks"
    shutil.copytree(notebooks_path, notebooks_dest)
    print(f"✅ Copied notebooks to {notebooks_dest}")
    
    # Copy documentation
    docs_to_copy = [
        "README.md",
        "docs/API_REFERENCE.md",
        "docs/USAGE_EXAMPLES.md",
        "docs/SCD_LIBRARY_SPECIFICATION.md",
        "docs/DATABRICKS_SETUP.md",
        "docs/DEPLOYMENT_SUMMARY.md",
        "docs/README.md"
    ]
    
    for doc in docs_to_copy:
        src_file = project_root / doc
        if src_file.exists():
            dest_file = output_path / doc
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_file, dest_file)
            print(f"✅ Copied {doc}")
    
    # Create deployment instructions
    instructions = """# Databricks Deployment Instructions

## Step 1: Upload to Databricks Workspace

1. Navigate to your Databricks workspace
2. Go to **Workspace** → **Users** → **your_username**
3. Create a folder called `dimensional_processing`
4. Upload the `dimensional_processing/` folder from this package

## Step 2: Upload Notebooks

1. Go to **Workspace** → **Users** → **your_username**
2. Create a folder called `notebooks`
3. Upload the notebooks from the `notebooks/` folder

## Step 3: Update Import Paths

In the notebooks, update the import path:
```python
sys.path.append('/Workspace/Users/your_username/dimensional_processing')
```

Replace `your_username` with your actual Databricks username.

## Step 4: Run Tests

1. Start with `Quick_SCD_Test.py` for quick validation
2. Run `SCD_Library_Testing_Notebook.py` for comprehensive testing

## File Structure in Databricks

```
/Workspace/Users/your_username/
├── dimensional_processing/
│   ├── __init__.py
│   ├── common/
│   ├── scd_type2/
│   └── key_resolution/
└── notebooks/
    ├── SCD_Library_Testing_Notebook.py
    ├── Quick_SCD_Test.py
    └── README.md
```

## Troubleshooting

- Ensure you have CREATE TABLE permissions
- Check that the import path is correct
- Verify your data schema matches the expected format
"""
    
    with open(output_path / "DEPLOYMENT_INSTRUCTIONS.md", "w") as f:
        f.write(instructions)
    
    print("✅ Created deployment instructions")
    
    # Create ZIP file
    zip_path = project_root / "databricks_scd_library.zip"
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(output_path):
            for file in files:
                file_path = Path(root) / file
                arc_path = file_path.relative_to(output_path)
                zipf.write(file_path, arc_path)
    
    print(f"✅ Created deployment package: {zip_path}")
    
    # Print summary
    print("\n📦 Package Contents:")
    print(f"   📁 dimensional_processing/ - Library source code")
    print(f"   📁 notebooks/ - Testing notebooks")
    print(f"   📁 docs/ - Complete documentation")
    print(f"   📄 README.md - Main documentation")
    print(f"   📄 DEPLOYMENT_INSTRUCTIONS.md - Deployment guide")
    
    print(f"\n🎉 Deployment package ready!")
    print(f"   📦 Package: {zip_path}")
    print(f"   📁 Unpacked: {output_path}")
    
    return zip_path, output_path

def print_deployment_summary():
    """Print deployment summary and next steps."""
    
    print("\n" + "="*60)
    print("🚀 DATABRICKS DEPLOYMENT READY!")
    print("="*60)
    
    print("\n📋 Next Steps:")
    print("1. 📦 Upload the ZIP file to your Databricks workspace")
    print("2. 📁 Extract the contents to your user folder")
    print("3. 🔧 Update import paths in the notebooks")
    print("4. 🧪 Run the test notebooks")
    print("5. 🎯 Start using the library in your projects!")
    
    print("\n📚 Documentation:")
    print("   • README.md - Complete library documentation")
    print("   • docs/ - Complete documentation directory")
    print("   • docs/README.md - Documentation index")
    print("   • docs/API_REFERENCE.md - Detailed API documentation")
    print("   • docs/USAGE_EXAMPLES.md - Usage examples")
    print("   • docs/DATABRICKS_SETUP.md - Databricks setup guide")
    print("   • DEPLOYMENT_INSTRUCTIONS.md - Step-by-step deployment")
    print("   • notebooks/README.md - Notebook usage guide")
    
    print("\n🧪 Testing:")
    print("   • Quick_SCD_Test.py - Fast validation (2-3 minutes)")
    print("   • SCD_Library_Testing_Notebook.py - Comprehensive testing (10-15 minutes)")
    
    print("\n✨ Features:")
    print("   • SCD Type 2 Processing")
    print("   • Historical Data Deduplication")
    print("   • Dimensional Key Resolution")
    print("   • Error Handling and Validation")
    print("   • Performance Optimization")
    print("   • Comprehensive Testing Suite")

if __name__ == "__main__":
    try:
        zip_path, output_path = create_databricks_package()
        print_deployment_summary()
    except Exception as e:
        print(f"❌ Error creating deployment package: {e}")
        raise
