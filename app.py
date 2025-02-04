from flask import Flask, request, render_template
from dotenv import load_dotenv
import pandas as pd
import openai
import os
import chardet

load_dotenv()
openai_api_key=os.getenv("OPENAI_API_KEY")

client = openai.OpenAI(api_key=openai_api_key) 
app = Flask(__name__, template_folder=".")

# Define and create upload folder
UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)  # Ensure folder exists

# Function to detect file encoding
def detect_encoding(file_path):
    with open(file_path, "rb") as f:
        result = chardet.detect(f.read(100000))  # Read a large enough chunk to detect encoding
    encoding = result["encoding"]
    if encoding is None:
        encoding = "utf-8"  # Default to UTF-8 if detection fails
    print(f"✅ Detected encoding: {encoding}")  # Debugging print
    return encoding

# Function to analyze expenses
def analyze_expenses(file_path):
    try:
        # ✅ Detect the correct encoding before reading the CSV
        detected_encoding = detect_encoding(file_path)

        # ✅ Read the CSV file using the detected encoding
        df = pd.read_csv(file_path, encoding=detected_encoding)

        # ✅ Convert column names to lowercase
        df.columns = df.columns.str.lower().str.strip()

        # ✅ Ensure required columns exist
        required_columns = {"expense name", "expense type", "amount"}
        if not required_columns.issubset(df.columns):
            return "❌ Error: The CSV file must include 'Expense Name', 'Expense Type', and 'Amount' columns."

        # ✅ Remove "$" from "Amount" column and convert to float
        df["amount"] = df["amount"].replace(r'[\$,]', '', regex=True).astype(float)

        # ✅ Categorize expenses as Essential vs Non-Essential
        essential_keywords = ["food", "rent", "utilities", "groceries", "transport", "medical"]
        df["category"] = df["expense type"].apply(
            lambda x: "Essential" if any(keyword in x.lower() for keyword in essential_keywords) else "Non-Essential"
        )

        # ✅ Calculate total non-essential spending
        total_non_essential = df[df["category"] == "Non-Essential"]["amount"].sum()

        # ✅ Format expenses for OpenAI
        essential_expenses = df[df["category"] == "Essential"][["expense name", "amount"]].to_string(index=False)
        non_essential_expenses = df[df["category"] == "Non-Essential"][["expense name", "amount"]].to_string(index=False)

        # ✅ OpenAI API request: Only categorize expenses
        prompt = f"""
        The following is a list of expenses, their categories, and amounts:

        **Essential Expenses:**
        {essential_expenses}

        **Non-Essential Expenses:**
        {non_essential_expenses}

        The total amount spent on **Non-Essential** expenses is: **${total_non_essential:.2f}**.

        Provide the output in the following format:

        ```
        Essential:
        - [Expense Name] ($Amount)

        Non-Essential:
        - [Expense Name] ($Amount)

        Total Non-Essential Spending: **$[Total Amount]**
        ```

        Please include insights on how to reduce the non-essential expenses.
        """

        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}]
        )

        return response.choices[0].message.content

    except Exception as e:
        return f"Error processing file: {str(e)}"

@app.route("/", methods=["GET", "POST"])
def index():
    analysis = "Upload your CSV file to analyze your expenses."

    if request.method == "POST":
        if "file" not in request.files:
            return "No file part"

        file = request.files["file"]

        if file.filename == "":
            return "No selected file"

        if file:
            # Save file safely
            file_path = os.path.join(UPLOAD_FOLDER, "uploaded_expense.csv")
            print(f"📂 Saving file to: {file_path}")  # Debugging log
            file.save(file_path)

            # Process the uploaded file
            analysis = analyze_expenses(file_path)

    return render_template("index.html", analysis=analysis)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8000)
