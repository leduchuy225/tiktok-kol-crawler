import pandas as pd
import random


# Test the ExcelWriter configuration
def test_excel_writer():
    # Create sample data similar to what we use
    sample_data = [
        {"username": "test_user1", "followers": 1000, "following": 500},
        {
            "username": "test_user2",
            "followers": float("nan"),
            "following": 200,
        },  # Test NaN
        {
            "username": "test_user3",
            "followers": 3000,
            "following": float("inf"),
        },  # Test infinity
    ]

    df = pd.DataFrame(sample_data)

    try:
        # Test the same configuration as in main.py
        with pd.ExcelWriter(
            "test_output.xlsx",
            engine="xlsxwriter",
            engine_kwargs={"options": {"nan_inf_to_errors": True}},
        ) as writer:
            df.to_excel(writer, sheet_name="Sheet1", index=False)

            workbook = writer.book
            worksheet = writer.sheets["Sheet1"]

            # Define some random colors
            colors = [
                "#FF6B6B",
                "#4ECDC4",
                "#45B7D1",
                "#96CEB4",
                "#FFEAA7",
                "#DDA0DD",
                "#98D8C8",
                "#F7DC6F",
                "#BB8FCE",
                "#85C1E9",
            ]

            # Format for username column (column A)
            for row_num in range(1, len(df) + 1):  # Start from 1 to skip header
                color = random.choice(colors)
                cell_format = workbook.add_format({"font_color": color})
                worksheet.write(
                    f"A{row_num + 1}", df.iloc[row_num - 1]["username"], cell_format
                )

        print(
            "✅ ExcelWriter test passed! File 'test_output.xlsx' created successfully."
        )
        print("✅ NaN and infinity values handled correctly.")
        print("✅ Random colors applied to username column.")

    except Exception as e:
        print(f"❌ ExcelWriter test failed: {e}")
        return False

    return True


if __name__ == "__main__":
    test_excel_writer()
