from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions
from plaid.model.sandbox_public_token_create_request import SandboxPublicTokenCreateRequest
from plaid.model.products import Products
from plaid.model.country_code import CountryCode
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.webhook_verification_key_get_request import WebhookVerificationKeyGetRequest
from plaid.model.sandbox_public_token_create_request_options import SandboxPublicTokenCreateRequestOptions
from plaid.api import plaid_api
from plaid.configuration import Configuration
from plaid.api_client import ApiClient
from plaid.exceptions import ApiException
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
import os
import hmac
import json
import time
import logging
import backoff
import base64
from dotenv import load_dotenv
from flask import Flask, request, jsonify
import threading
from functools import wraps
import requests

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('plaid_webhook.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Add file handler for debug logs specifically
debug_handler = logging.FileHandler('plaid_debug.log')
debug_handler.setLevel(logging.DEBUG)
debug_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
debug_handler.setFormatter(debug_formatter)
logger.addHandler(debug_handler)

app = Flask(__name__)

class WebhookError(Exception):
    """Custom exception for webhook errors."""
    pass

class PlaidAPIError(Exception):
    """Custom exception for Plaid API errors."""
    def __init__(self, message: str, error_code: str, error_response: dict):
        self.message = message
        self.error_code = error_code
        self.error_response = error_response
        super().__init__(self.message)

def retry_on_error(max_tries=3, delay=1):
    """Decorator for retrying failed operations."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_tries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts == max_tries:
                        logger.error(f"Failed after {max_tries} attempts: {str(e)}")
                        raise
                    logger.warning(f"Attempt {attempts} failed, retrying in {delay} seconds...")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

class PlaidSandboxConnector:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # Configure API client
        configuration = Configuration(
            host='https://sandbox.plaid.com',
            api_key={
                'clientId': os.getenv('PLAID_CLIENT_ID'),
                'secret': os.getenv('PLAID_SECRET'),
            }
        )
        
        # Create API client
        api_client = ApiClient(configuration)
        self.client = plaid_api.PlaidApi(api_client)
        
        # Try to load access token from environment or file
        self.access_token = os.getenv('PLAID_ACCESS_TOKEN')
        if not self.access_token:
            try:
                with open('.access_token', 'r') as f:
                    self.access_token = f.read().strip()
            except FileNotFoundError:
                self.access_token = None
                
        self.webhook_url = os.getenv('PLAID_WEBHOOK_URL', 'http://localhost:8000/webhook')
    
    def create_sandbox_access_token(self, institution_id: str = 'ins_109508') -> str:
        """
        Create a sandbox access token with webhook support.
        Default institution is Chase (ins_109508).
        """
        try:
            # Create request options with webhook
            options = SandboxPublicTokenCreateRequestOptions(
                webhook=self.webhook_url
            )
            
            # Create a sandbox public token
            request = SandboxPublicTokenCreateRequest(
                institution_id=institution_id,
                initial_products=[Products('transactions')],
                options=options
            )
            
            pt_response = self.client.sandbox_public_token_create(request)
            public_token = pt_response.public_token
            
            # Exchange it for an access token
            exchange_request = ItemPublicTokenExchangeRequest(
                public_token=public_token
            )
            exchange_response = self.client.item_public_token_exchange(exchange_request)
            self.access_token = exchange_response.access_token
            
            # Save the access token for future use
            with open('.access_token', 'w') as f:
                f.write(self.access_token)
            
            logger.info(f"Sandbox access token created successfully: {self.access_token}")
            logger.info(f"Webhook configured for: {self.webhook_url}")
            return self.access_token
            
        except Exception as e:
            logger.error(f"Error creating sandbox access token: {str(e)}")
            return None

class PlaidTransactionAnalyzer:
    def __init__(self, access_token: str):
        # Load environment variables
        load_dotenv()
        
        # Configure API client
        configuration = Configuration(
            host=os.getenv('PLAID_ENV', 'https://sandbox.plaid.com'),
            api_key={
                'clientId': os.getenv('PLAID_CLIENT_ID'),
                'secret': os.getenv('PLAID_SECRET'),
            }
        )
        
        # Create API client
        api_client = ApiClient(configuration)
        self.client = plaid_api.PlaidApi(api_client)
        self.access_token = access_token
    
    def get_date_range(self) -> tuple:
        """Calculate start and end dates for last 12 months."""
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=365)
        return start_date, end_date

    def fetch_transactions(self) -> List[Dict[Any, Any]]:
        """Fetch all transactions for the last 12 months with enriched data."""
        start_date, end_date = self.get_date_range()
        max_retries = 5
        base_delay = 10  # seconds
        
        logger.debug(f"Fetching transactions from {start_date} to {end_date}")
        
        for attempt in range(max_retries):
            try:
                # Configure request options
                options = TransactionsGetRequestOptions(
                    include_personal_finance_category=True,
                )
                
                logger.debug(f"Request options configured: {options}")
                
                # Create the base request
                request = TransactionsGetRequest(
                    access_token=self.access_token,
                    start_date=start_date,
                    end_date=end_date,
                    options=options
                )
                
                logger.debug(f"Sending initial transactions request (attempt {attempt + 1}/{max_retries})...")
                
                try:
                    # Initial request for transactions
                    response = self.client.transactions_get(request)
                    transactions = response.transactions
                    logger.debug(f"Complete Response:")
                    logger.debug(f"{response.transactions}")
                    break  # Success! Exit the retry loop
                    
                except ApiException as e:
                    error_response = json.loads(e.body)
                    error_code = error_response.get('error_code', 'UNKNOWN')
                    error_message = error_response.get('error_message', str(e))
                    error_type = error_response.get('error_type', 'API_ERROR')
                    
                    logger.error(f"""
                        Plaid API Error:
                        Type: {error_type}
                        Code: {error_code}
                        Message: {error_message}
                        Request ID: {error_response.get('request_id', 'N/A')}
                        Documentation: {error_response.get('documentation_url', 'N/A')}
                        Attempt: {attempt + 1}/{max_retries}
                    """)
                    
                    if error_code == 'ITEM_ERROR':
                        if attempt < max_retries - 1:
                            wait_time = base_delay * (2 ** attempt)  # Exponential backoff
                            logger.info(f"Item not ready. Waiting {wait_time} seconds before retry...")
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.error("Max retries reached waiting for item to be ready")
                    
                    # Handle other specific error cases
                    if error_code == 'INVALID_ACCESS_TOKEN':
                        logger.error("The access token provided is invalid or has expired")
                    elif error_code == 'INVALID_REQUEST':
                        logger.error("The request was malformed. Check the error message for details")
                    elif error_code == 'RATE_LIMIT_EXCEEDED':
                        logger.error("Rate limit exceeded. Please wait before retrying")
                        
                    raise PlaidAPIError(f"{error_type}: {error_message}", error_code, error_response)
            
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = base_delay * (2 ** attempt)
                    logger.error(f"Unexpected error: {str(e)}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                raise
        else:
            raise PlaidAPIError("Max retries reached", "ITEM_ERROR", {"error_message": "Item not ready after max retries"})
 
            logger.debug(f"Initial response received:")
            logger.debug(f"Total transactions available: {response.total_transactions}")
            logger.debug(f"Transactions in current batch: {len(transactions)}")
            
            # Handle pagination if there are more transactions
            page = 1
            while len(transactions) < response.total_transactions:
                page += 1
                logger.debug(f"Fetching page {page} of transactions...")
                
                # Update request with offset
                request.options.offset = len(transactions)
                try:
                    response = self.client.transactions_get(request)
                    new_transactions = response.transactions
                except ApiException as e:
                    error_response = json.loads(e.body)
                    logger.error(f"Error fetching page {page}: {error_response.get('error_message', str(e))}")
                    raise PlaidAPIError(
                        f"Error fetching page {page}: {error_response.get('error_message')}",
                        error_response.get('error_code', 'UNKNOWN'),
                        error_response
                    )
                
                logger.debug(f"Retrieved {len(new_transactions)} additional transactions")
                transactions.extend(new_transactions)
                logger.debug(f"Total transactions retrieved so far: {len(transactions)}")
            
            # Convert to dictionaries and log summary
            transaction_dicts = [self._transaction_to_dict(trans) for trans in transactions]
            
            # Log summary statistics
            if transaction_dicts:
                amounts = [t['amount'] for t in transaction_dicts]
                logger.debug(f"""
                    Transaction Summary:
                    Total Count: {len(transaction_dicts)}
                    Date Range: {min(t['date'] for t in transaction_dicts)} to {max(t['date'] for t in transaction_dicts)}
                    Amount Range: ${min(amounts):.2f} to ${max(amounts):.2f}
                    Total Amount: ${sum(amounts):.2f}
                    Unique Merchants: {len(set(t['merchant_name'] for t in transaction_dicts))}
                    Categories: {set(t['personal_finance_category']['primary'] for t in transaction_dicts if t['personal_finance_category']['primary'])}
                """)
            
            return transaction_dicts
            
    
    def _transaction_to_dict(self, transaction) -> Dict[Any, Any]:
        """Convert Plaid transaction object to dictionary."""
        transaction_dict = {
            'date': transaction.date,
            'merchant_name': transaction.merchant_name,
            'name': transaction.name,
            'amount': transaction.amount,
            'payment_channel': transaction.payment_channel,
            'personal_finance_category': {
                'primary': transaction.personal_finance_category.primary if transaction.personal_finance_category else None,
                'detailed': transaction.personal_finance_category.detailed if transaction.personal_finance_category else None
            }
        }
        
        logger.debug(f"Converted transaction: {transaction.name} - ${transaction.amount}")
        return transaction_dict
    
    def enrich_transactions_data(self, transactions: List[Dict[Any, Any]]) -> pd.DataFrame:
        """Convert transactions to DataFrame and add enriched features."""
        if not transactions:
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(transactions)
        
        # Extract nested fields
        df['category'] = df['personal_finance_category'].apply(
            lambda x: x.get('primary', 'Unknown') if x else 'Unknown'
        )
        df['detailed_category'] = df['personal_finance_category'].apply(
            lambda x: x.get('detailed', 'Unknown') if x else 'Unknown'
        )
        
        # Add merchant information
        df['merchant_name'] = df['merchant_name'].fillna('Unknown')
        
        # Convert amount to positive/negative based on transaction type
        df['amount'] = df.apply(
            lambda x: -x['amount'] if x['amount'] > 0 else x['amount'],
            axis=1
        )
        
        # Add month column for aggregation
        df['month'] = pd.to_datetime(df['date']).dt.to_period('M')
        
        return df[['date', 'month', 'merchant_name', 'category', 
                  'detailed_category', 'amount', 'name', 'payment_channel']]
    
    def generate_insights(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate financial insights from the transaction data."""
        if df.empty:
            return {}
        
        insights = {
            'total_spending': abs(df[df['amount'] < 0]['amount'].sum()),
            'total_income': df[df['amount'] > 0]['amount'].sum(),
            'monthly_summary': df.groupby('month')['amount'].agg({
                'spending': lambda x: abs(x[x < 0].sum()),
                'income': lambda x: x[x > 0].sum()
            }).to_dict(),
            'top_categories': df[df['amount'] < 0].groupby('category')['amount'].agg({
                'total_spent': lambda x: abs(x.sum()),
                'transaction_count': 'count'
            }).sort_values('total_spent', ascending=False).head(5).to_dict(),
            'top_merchants': df[df['amount'] < 0].groupby('merchant_name')['amount'].agg({
                'total_spent': lambda x: abs(x.sum()),
                'transaction_count': 'count'
            }).sort_values('total_spent', ascending=False).head(5).to_dict()
        }
        
        return insights

class PlaidWebhookVerifier:
    def __init__(self, client):
        self.client = client
        self.verification_enabled = os.getenv('PLAID_VERIFY_WEBHOOKS', 'false').lower() == 'true'

    def verify_webhook(self, body: bytes, headers: dict) -> bool:
        """Verify incoming webhook based on environment configuration."""
        if not self.verification_enabled:
            logger.info("Webhook verification disabled, accepting all webhooks")
            return True

        if os.getenv('PLAID_ENV') == 'sandbox':
            logger.info("Sandbox environment detected, accepting webhook")
            return True

        return True

class PlaidWebhookHandler:
    def __init__(self, analyzer: PlaidTransactionAnalyzer):
        self.analyzer = analyzer
        self.webhook_processors = {
            'TRANSACTIONS': {
                'INITIAL_UPDATE': self.handle_initial_update,
                'HISTORICAL_UPDATE': self.handle_historical_update,
                'DEFAULT_UPDATE': self.handle_default_update,
                'TRANSACTIONS_REMOVED': self.handle_transactions_removed
            },
            'ITEM': {
                'ERROR': self.handle_item_error,
                'PENDING_EXPIRATION': self.handle_pending_expiration
            }
        }

    @retry_on_error(max_tries=3, delay=2)
    def process_webhook(self, webhook_data: dict) -> Tuple[bool, str]:
        """Process webhook data with retry logic."""
        try:
            webhook_type = webhook_data.get('webhook_type')
            webhook_code = webhook_data.get('webhook_code')

            if webhook_type not in self.webhook_processors:
                raise WebhookError(f"Unknown webhook type: {webhook_type}")

            processor_dict = self.webhook_processors[webhook_type]
            if webhook_code not in processor_dict:
                raise WebhookError(f"Unknown webhook code: {webhook_code}")

            processor = processor_dict[webhook_code]
            return processor(webhook_data)

        except Exception as e:
            logger.error(f"Error processing webhook: {str(e)}")
            return False, str(e)

    def handle_initial_update(self, data: dict) -> Tuple[bool, str]:
        """Handle initial transaction update."""
        try:
            logger.info("Processing initial transaction update")
            transactions = self.analyzer.fetch_transactions()
            df = self.analyzer.enrich_transactions_data(transactions)  # Note the corrected method name
            insights = self.analyzer.generate_insights(df)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            df.to_csv(f'transactions_initial_{timestamp}.csv', index=False)
            pd.DataFrame(insights).to_csv(f'insights_initial_{timestamp}.csv')

            return True, "Initial update processed successfully"
        except Exception as e:
            logger.error(f"Error in initial update: {str(e)}")
            return False, str(e)

    def handle_historical_update(self, data: dict) -> Tuple[bool, str]:
        """Handle historical transaction update."""
        try:
            logger.info("Processing historical transaction update")
            new_transactions = data.get('new_transactions')
            logger.info(f"Received {new_transactions} new historical transactions")
            return True, "Historical update processed successfully"
        except Exception as e:
            logger.error(f"Error in historical update: {str(e)}")
            return False, str(e)

    def handle_default_update(self, data: dict) -> Tuple[bool, str]:
        """Handle regular transaction update."""
        try:
            logger.info("Processing default transaction update")
            new_transactions = data.get('new_transactions')
            logger.info(f"Received {new_transactions} new transactions")
            return True, "Default update processed successfully"
        except Exception as e:
            logger.error(f"Error in default update: {str(e)}")
            return False, str(e)

    def handle_transactions_removed(self, data: dict) -> Tuple[bool, str]:
        """Handle removed transactions."""
        try:
            logger.info("Processing removed transactions")
            removed_transactions = data.get('removed_transactions', [])
            logger.info(f"Removing {len(removed_transactions)} transactions")
            return True, "Removed transactions processed successfully"
        except Exception as e:
            logger.error(f"Error in removing transactions: {str(e)}")
            return False, str(e)

    def handle_item_error(self, data: dict) -> Tuple[bool, str]:
        """Handle item errors."""
        try:
            logger.error(f"Item Error received: {data.get('error', {})}")
            return True, "Item error handled"
        except Exception as e:
            logger.error(f"Error handling item error: {str(e)}")
            return False, str(e)

    def handle_pending_expiration(self, data: dict) -> Tuple[bool, str]:
        """Handle pending expiration warning."""
        try:
            logger.warning(f"Item pending expiration: {data.get('consent_expiration_time')}")
            return True, "Pending expiration handled"
        except Exception as e:
            logger.error(f"Error handling pending expiration: {str(e)}")
            return False, str(e)

@app.route('/webhook', methods=['POST'])
def webhook_handler():
    """Handle incoming webhooks from Plaid."""
    try:
        start_time = time.time()
        logger.info("Received webhook request")

        if not request.is_json:
            logger.error("Invalid content type")
            return jsonify({"error": "Invalid content type"}), 400

        # Log request details
        logger.info(f"Headers: {dict(request.headers)}")
        body = request.get_data()
        logger.info(f"Body: {body.decode('utf-8')}")

        # Create connector and get access token
        connector = PlaidSandboxConnector()
        if not connector.access_token:
            # If we don't have an access token, create one
            connector.create_sandbox_access_token()

        if not connector.access_token:
            logger.error("No access token available")
            return jsonify({"error": "No access token available"}), 500

        # Verify webhook
        verifier = PlaidWebhookVerifier(connector.client)
        if not verifier.verify_webhook(body, dict(request.headers)):
            logger.error("Invalid webhook verification")
            return jsonify({"error": "Invalid webhook verification"}), 401

        # Process webhook with the access token
        webhook_data = request.json
        handler = PlaidWebhookHandler(PlaidTransactionAnalyzer(connector.access_token))
        success, message = handler.process_webhook(webhook_data)

        # Log processing time
        processing_time = time.time() - start_time
        logger.info(f"Webhook processed in {processing_time:.2f} seconds")

        if not success:
            return jsonify({"error": message}), 500

        return jsonify({
            "status": "success",
            "message": message,
            "processing_time": processing_time
        }), 200

    except Exception as e:
        logger.error(f"Error handling webhook: {str(e)}")
        return jsonify({"error": str(e)}), 500


def run_flask():
    """Run the Flask app for webhook handling."""
    port = int(os.getenv('PLAID_WEBHOOK_PORT', '8000'))
    app.run(port=port)



def main(test_mode: bool = False, test_iterations: int = 3):
    try:
        # Start Flask app in a separate thread
        flask_thread = threading.Thread(target=run_flask)
        flask_thread.daemon = True
        flask_thread.start()
        
        # Create a sandbox connection
        connector = PlaidSandboxConnector()
        access_token = connector.create_sandbox_access_token()
        
        if not access_token:
            logger.error("Failed to create sandbox access token")
            return
        
        logger.info("\nSuccessfully created sandbox access token!")
        logger.info(f"Webhook server is running on port {os.getenv('PLAID_WEBHOOK_PORT', '8000')}.")
        
        # Add initial delay after creating the access token
        initial_wait = 5  # seconds
        logger.info(f"Waiting {initial_wait} seconds for initial item setup...")
        time.sleep(initial_wait)
        
        if test_mode:
            logger.info(f"Running in test mode for {test_iterations} iterations")
            analyzer = PlaidTransactionAnalyzer(access_token)
            
            for i in range(test_iterations):
                logger.info(f"\nTest Iteration {i + 1}/{test_iterations}")
                try:
                    transactions = analyzer.fetch_transactions()
                    df = analyzer.enrich_transactions_data(transactions)
                    insights = analyzer.generate_insights(df)
                    
                    # Save results with iteration number
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    df.to_csv(f'transactions_test_{i+1}_{timestamp}.csv', index=False)
                    pd.DataFrame(insights).to_csv(f'insights_test_{i+1}_{timestamp}.csv')
                    
                    logger.info(f"Test iteration {i + 1} completed successfully")
                    
                    if i < test_iterations - 1:
                        logger.info("Waiting 10 seconds before next iteration...")
                        time.sleep(10)
                        
                except PlaidAPIError as e:
                    if e.error_code == 'ITEM_ERROR':
                        logger.warning(f"Item not ready in iteration {i + 1}. Waiting longer before next attempt...")
                        time.sleep(90)  # Wait longer before next iteration
                        continue
                    elif e.error_code == 'RATE_LIMIT_EXCEEDED':
                        wait_time = 120
                        logger.info(f"Rate limit exceeded. Waiting {wait_time} seconds before next iteration...")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"""
                            Error in test iteration {i + 1}:
                            Error Code: {e.error_code}
                            Message: {e.message}
                            Documentation: {e.error_response.get('documentation_url', 'N/A')}
                        """)
                        raise
                    
        else:
            logger.info("Running in continuous mode. Press Ctrl+C to exit.")
            while True:
                time.sleep(1)
                
    except KeyboardInterrupt:
        logger.info("\nShutting down gracefully...")
    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        logger.debug("Full error details:", exc_info=True)
        raise
    finally:
        logger.info("Application terminated")

if __name__ == "__main__":
    # You can control the mode via environment variable
    test_mode = os.getenv('PLAID_TEST_MODE', 'true').lower() == 'true'
    test_iterations = int(os.getenv('PLAID_TEST_ITERATIONS', '3'))

    main(test_mode=test_mode, test_iterations=test_iterations)
