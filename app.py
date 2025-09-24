import os
import logging
from datetime import datetime
import pandas as pd
from flask import Flask, render_template, request, jsonify, send_file
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from num2words import num2words

# --- Basic Logging Setup ---
# This will help in debugging issues in a live environment
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# --- App Configuration ---
app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'invoice.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# --- Database Models ---
class Client(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    address = db.Column(db.String(200))
    mobile = db.Column(db.String(20))
    email = db.Column(db.String(50))
    alt_mobile = db.Column(db.String(20))

class Item(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    description = db.Column(db.String(200), unique=True, nullable=False)
    hsn_code = db.Column(db.String(20), nullable=True)
    unit = db.Column(db.String(10), nullable=True)
    last_rate = db.Column(db.Float)

class Invoice(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    invoice_number = db.Column(db.String(50), unique=True, nullable=False)
    date = db.Column(db.DateTime, nullable=False)
    client_id = db.Column(db.Integer, db.ForeignKey('client.id'), nullable=False)
    subtotal = db.Column(db.Float, nullable=False)
    transport = db.Column(db.Float, default=0.0)
    total = db.Column(db.Float, nullable=False)
    client = db.relationship('Client', backref='invoices')
    items = db.relationship('InvoiceItem', backref='invoice', cascade='all, delete-orphan')

class InvoiceItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    invoice_id = db.Column(db.Integer, db.ForeignKey('invoice.id'), nullable=False)
    description = db.Column(db.String(200), nullable=False)
    hsn_code = db.Column(db.String(20), nullable=True)
    quantity = db.Column(db.Float, nullable=False)
    unit = db.Column(db.String(10), nullable=True)
    rate = db.Column(db.Float, nullable=False)
    amount = db.Column(db.Float, nullable=False)

# --- Create database tables within application context ---
with app.app_context():
    db.create_all()

# --- Template Filters ---
@app.template_filter('in_words')
def in_words_filter(amount):
    """Converts a numeric amount to words in Indian currency format."""
    try:
        amount = round(float(amount), 2)
    except (ValueError, TypeError):
        return "Invalid Amount"
    
    integer_part = int(amount)
    fractional_part = int(round((amount - integer_part) * 100))
    
    if integer_part == 0 and fractional_part == 0:
        return "Zero Rupees Only"
    
    words = num2words(integer_part, lang='en_IN').title()
    if fractional_part > 0:
        words += " And " + num2words(fractional_part, lang='en_IN').title() + " Paisa"
        
    return "Indian Rupees " + words + ' Only'

# --- Helper Functions ---
def generate_invoice_number():
    """Generates a sequential invoice number based on the current month and year."""
    month_codes = {
        1: 'JA', 2: 'FE', 3: 'MR', 4: 'AP', 5: 'MY', 6: 'JN',
        7: 'JL', 8: 'AU', 9: 'SE', 10: 'OC', 11: 'NO', 12: 'DE'
    }
    today = datetime.now()
    month_code = month_codes[today.month]
    # Using last two digits of the year for a shorter prefix
    prefix = f"{today.strftime('%y')}{month_code}-"
    
    with app.app_context():
        last_invoice = db.session.query(Invoice.invoice_number).filter(
            Invoice.invoice_number.like(f"{prefix}%")
        ).order_by(Invoice.invoice_number.desc()).first()
        
        if last_invoice:
            last_seq = int(last_invoice[0].split("-")[1])
            new_seq = last_seq + 1
        else:
            new_seq = 1
            
        return f"{prefix}{new_seq:03d}"

# --- Main Routes ---
@app.route('/')
def home():
    """Renders the main invoice creation page."""
    return render_template('create_invoice.html', invoice_number=generate_invoice_number())

# --- API Routes for Frontend ---
@app.route('/get_client/<int:client_id>')
def get_client(client_id):
    """Fetches details for a specific client by ID."""
    client = db.get_or_404(Client, client_id)
    return jsonify({
        'name': client.name,
        'address': client.address,
        'mobile': client.mobile,
        'email': client.email,
        'alt_mobile': client.alt_mobile
    })

@app.route('/search_clients')
def search_clients():
    """Provides client search results for autocomplete fields."""
    query = request.args.get('q', '')
    clients = Client.query.filter(Client.name.ilike(f'%{query}%')).limit(10).all()
    return jsonify([{'id': c.id, 'text': c.name} for c in clients])

@app.route('/search_items')
def search_items():
    """Provides item search results with all necessary data for the invoice form."""
    query = request.args.get('q', '')
    items = Item.query.filter(Item.description.ilike(f'%{query}%')).limit(10).all()
    return jsonify([{
        'id': i.id, 
        'text': i.description, 
        'rate': i.last_rate,
        'hsn_code': i.hsn_code,
        'unit': i.unit
    } for i in items])

@app.route('/create_invoice', methods=['POST'])
def create_invoice():
    """Handles the creation of a new invoice, client, and items."""
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'Invalid data received'}), 400

    try:
        # --- 1. Handle Client: Find, Update, or Create ---
        client_data = data['client']
        client_name = client_data.get('name', '').strip()
        if not client_name:
            return jsonify({'success': False, 'error': 'Client name cannot be empty'}), 400

        client = Client.query.filter(func.lower(Client.name) == func.lower(client_name)).first()

        if client:
            # If client exists, update their details from the form
            client.address = client_data.get('address', client.address)
            client.mobile = client_data.get('mobile', client.mobile)
            client.email = client_data.get('email', client.email)
            client.alt_mobile = client_data.get('alt_mobile', client.alt_mobile)
        else:
            # If client is new, create them
            client = Client(
                name=client_name,
                address=client_data.get('address', ''),
                mobile=client_data.get('mobile', ''),
                email=client_data.get('email', ''),
                alt_mobile=client_data.get('alt_mobile', '')
            )
        db.session.add(client)
        
        # --- 2. Create Invoice ---
        invoice = Invoice(
            invoice_number=generate_invoice_number(),
            date=datetime.strptime(data['date'], '%Y-%m-%d'),
            client=client, # Use the relationship directly
            subtotal=data['subtotal'],
            transport=data.get('transport', 0),
            total=data['total']
        )
        db.session.add(invoice)

        # --- 3. Handle Items: Add to Invoice and Update Master List ---
        for item_data in data['items']:
            # Add or update the item in the master Item table
            item_description = item_data.get('description', '').strip()
            if not item_description:
                continue # Skip empty item rows

            master_item = Item.query.filter(func.lower(Item.description) == func.lower(item_description)).first()
            if not master_item:
                master_item = Item(
                    description=item_description,
                    hsn_code=item_data.get('hsn_code'),
                    unit=item_data.get('unit'),
                    last_rate=item_data['rate']
                )
            else:
                # Always update the last used rate for the item
                master_item.last_rate = item_data['rate']
            db.session.add(master_item)

            # Add the item to the current invoice
            inv_item = InvoiceItem(
                invoice=invoice, # Use relationship
                description=item_description,
                hsn_code=item_data.get('hsn_code', ''),
                quantity=item_data['quantity'],
                unit=item_data.get('unit', 'Nos'),
                rate=item_data['rate'],
                amount=item_data['amount']
            )
            db.session.add(inv_item)
            
        # --- 4. Commit Transaction ---
        # All operations are wrapped in a single transaction.
        # If any step fails, the entire transaction is rolled back.
        db.session.commit()
        
        logging.info(f"Successfully created invoice {invoice.invoice_number} for client {client.name}.")
        return jsonify({'success': True, 'invoice_id': invoice.id})

    except IntegrityError as e:
        db.session.rollback()
        logging.error(f"Database integrity error during invoice creation: {e}")
        # This could be a duplicate invoice number due to a race condition.
        return jsonify({'success': False, 'error': 'A database error occurred. It might be a duplicate entry. Please try again.'}), 409
    except Exception as e:
        db.session.rollback()
        logging.error(f"An unexpected error occurred during invoice creation: {e}", exc_info=True)
        return jsonify({'success': False, 'error': f'An unexpected error occurred: {str(e)}'}), 500


@app.route('/view_invoice/<int:invoice_id>')
def view_invoice(invoice_id):
    """Displays a generated invoice for printing or viewing."""
    invoice = db.get_or_404(Invoice, invoice_id)
    return render_template('invoice_template.html', invoice=invoice)

# --- Data Management Routes ---
@app.route('/export_clients')
def export_clients():
    """Exports all clients to an Excel file."""
    clients = Client.query.all()
    df = pd.DataFrame(
        [(c.name, c.address, c.mobile, c.email, c.alt_mobile) for c in clients],
        columns=['Name', 'Address', 'Mobile', 'Email', 'Alt Mobile']
    )
    file_path = 'clients_export.xlsx'
    df.to_excel(file_path, index=False)
    return send_file(file_path, as_attachment=True)

@app.route('/export_items')
def export_items():
    """Exports all master items to an Excel file."""
    items = Item.query.all()
    df = pd.DataFrame(
        [(i.description, i.hsn_code or '', i.unit or 'Nos', i.last_rate or 0) for i in items],
        columns=['Description', 'HSN Code', 'Unit', 'Last Rate']
    )
    file_path = 'items_export.xlsx'
    df.to_excel(file_path, index=False)
    return send_file(file_path, as_attachment=True)

@app.route('/import_data', methods=['POST'])
def import_data():
    """Imports clients or items from an Excel file with flexible column mapping."""
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file part'})
    file = request.files['file']
    data_type = request.form['type']
    
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No selected file'})

    if file and file.filename.endswith('.xlsx'):
        try:
            df = pd.read_excel(file)
            df = df.astype(str).replace('nan', '') # Normalize data

            if data_type == 'clients':
                # Logic to find and add new clients
                existing_clients = {name.lower() for name, in db.session.query(Client.name)}
                new_clients = []
                for _, row in df.iterrows():
                    name = row.get('Name', '').strip()
                    if name and name.lower() not in existing_clients:
                        new_clients.append(Client(
                            name=name,
                            address=row.get('Address', ''),
                            mobile=row.get('Mobile', ''),
                            email=row.get('Email', ''),
                            alt_mobile=row.get('Alt Mobile', '')
                        ))
                        existing_clients.add(name.lower())
                db.session.bulk_save_objects(new_clients)

            elif data_type == 'items':
                # Logic to find and add new items
                existing_items = {desc.lower() for desc, in db.session.query(Item.description)}
                new_items = []
                for _, row in df.iterrows():
                    description = row.get('Description', '').strip()
                    if description and description.lower() not in existing_items:
                        new_items.append(Item(
                            description=description,
                            hsn_code=row.get('HSN Code', ''),
                            unit=row.get('Unit', 'Nos'),
                            last_rate=pd.to_numeric(row.get('Last Rate'), errors='coerce') or 0
                        ))
                        existing_items.add(description.lower())
                db.session.bulk_save_objects(new_items)
            
            db.session.commit()
            return jsonify({'success': True})
        
        except Exception as e:
            db.session.rollback()
            logging.error(f"Error processing imported file: {e}", exc_info=True)
            return jsonify({'success': False, 'error': f"Error processing file: {str(e)}"})
    
    return jsonify({'success': False, 'error': 'Invalid file format. Please upload an Excel (.xlsx) file'})


if __name__ == '__main__':
    # Note: For production, use a proper WSGI server like Gunicorn or Waitress
    app.run(debug=True)
