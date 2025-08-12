from flask import Blueprint, request, jsonify
import json
from datetime import datetime
from config.snowflake_config import get_snowflake_session

catalog_bp = Blueprint('catalog', __name__)

@catalog_bp.route('/api/entities', methods=['GET', 'POST'])
def handle_entities():
    """Handle entity CRUD operations"""
    if request.method == 'GET':
        return jsonify(get_entities_from_snowflake())
    else:
        entity_data = request.json
        result = add_entity_to_snowflake(entity_data)
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400

@catalog_bp.route('/api/entities/<entity_id>', methods=['PUT', 'DELETE'])
def handle_entity(entity_id):
    """Handle single entity operations"""
    if request.method == 'PUT':
        entity_data = request.json
        result = update_entity_in_snowflake(entity_id, entity_data)
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400
    else:
        result = delete_entity_from_snowflake(entity_id)
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400

@catalog_bp.route('/api/vendors', methods=['GET', 'POST'])
def handle_vendors():
    """Handle vendor CRUD operations"""
    if request.method == 'GET':
        return jsonify(get_vendors_from_snowflake())
    else:
        vendor_data = request.json
        result = add_vendor_to_snowflake(vendor_data)
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400

@catalog_bp.route('/api/vendors/<vendor_name>', methods=['PUT', 'DELETE'])
def handle_vendor(vendor_name):
    """Handle single vendor operations"""
    if request.method == 'PUT':
        vendor_data = request.json
        result = update_vendor_in_snowflake(vendor_name, vendor_data)
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400
    else:
        result = delete_vendor_from_snowflake(vendor_name)
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400

# =====================================================
# NEW: VENDOR MAPPING ENDPOINTS
# =====================================================

@catalog_bp.route('/api/vendor-mappings', methods=['GET', 'POST'])
def handle_vendor_mappings():
    """Handle vendor mapping CRUD operations"""
    if request.method == 'GET':
        return jsonify(get_vendor_mappings_from_snowflake())
    else:
        mapping_data = request.json
        result = add_vendor_mapping_to_snowflake(mapping_data)
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400

@catalog_bp.route('/api/vendor-mappings/<mapping_id>', methods=['PUT', 'DELETE'])
def handle_vendor_mapping(mapping_id):
    """Handle single vendor mapping operations"""
    if request.method == 'PUT':
        mapping_data = request.json
        result = update_vendor_mapping_in_snowflake(mapping_id, mapping_data)
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400
    else:
        result = delete_vendor_mapping_from_snowflake(mapping_id)
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400

@catalog_bp.route('/api/entities-for-dropdown', methods=['GET'])
def get_entities_for_dropdown():
    """Get entities formatted for dropdown display"""
    try:
        entities = get_entities_from_snowflake()
        dropdown_entities = [
            {
                "id": entity["entity_id"],
                "display": f"{entity['entity_id']} - {entity['name']}"
            }
            for entity in entities if entity["status"] == "Active"
        ]
        return jsonify(dropdown_entities)
    except Exception as e:
        print(f"Error getting entities for dropdown: {e}")
        return jsonify([])

@catalog_bp.route('/api/vendors-for-dropdown', methods=['GET'])
def get_vendors_for_dropdown():
    """Get vendors formatted for dropdown display"""
    try:
        vendors = get_vendors_from_snowflake()
        dropdown_vendors = [
            {
                "id": vendor["name"],
                "display": vendor['name']
            }
            for vendor in vendors if vendor["status"] == "Active"
        ]
        return jsonify(dropdown_vendors)
    except Exception as e:
        print(f"Error getting vendors for dropdown: {e}")
        return jsonify([])

# =====================================================
# VENDOR MAPPING FUNCTIONS
# =====================================================

def get_vendor_mappings_from_snowflake():
    """Get all vendor mappings with entity and vendor details"""
    try:
        session = get_snowflake_session()
        
        # Join with catalog tables to get names
        result = session.sql("""
            SELECT 
                m.MAPPING_ID,
                m.ENTITY_ID,
                m.VENDOR_NAME,
                m.ENTITY_VENDOR_CODE,
                m.STATUS,
                m.CREATED_AT,
                m.UPDATED_AT,
                e.ENTITY_NAME,
                v.VENDOR_NAME as VENDOR_DISPLAY_NAME,
                v.CURRENCY
            FROM ENTITY_VENDOR_MAPPING m
            LEFT JOIN ENTITY_CATALOG e ON m.ENTITY_ID = e.ENTITY_ID
            LEFT JOIN VENDOR_CATALOG v ON m.VENDOR_NAME = v.VENDOR_NAME
            ORDER BY e.ENTITY_NAME, v.VENDOR_NAME
        """).collect()
        
        mappings = []
        for row in result:
            mappings.append({
                "mapping_id": row[0],
                "entity_id": row[1],
                "vendor_name": row[2],  # This is vendor_name
                "entity_vendor_code": row[3],
                "status": row[4],
                "created_at": str(row[5]) if row[5] else None,
                "updated_at": str(row[6]) if row[6] else None,
                "entity_name": row[7],
                "vendor_name": row[8] or row[2],
                "vendor_currency": row[9]
            })
        
        return mappings
        
    except Exception as e:
        print(f"Error getting vendor mappings from Snowflake: {e}")
        return []

def add_vendor_mapping_to_snowflake(mapping_data):
    """Add new vendor mapping to Snowflake"""
    try:
        # Validate required fields
        if not all([mapping_data.get('entity_id'), mapping_data.get('vendor_name'), 
                   mapping_data.get('entity_vendor_code')]):
            return {"success": False, "error": "Entity, vendor, and vendor code are required"}
        
        # Clean input
        entity_id = mapping_data['entity_id'].strip()
        vendor_name = mapping_data['vendor_name'].strip()  # This is actually vendor_name
        vendor_code = mapping_data['entity_vendor_code'].strip()
        
        # Check for duplicate mapping
        duplicate_check = check_mapping_duplicates(entity_id, vendor_name)
        if duplicate_check["is_duplicate"]:
            return {"success": False, "error": duplicate_check["error"]}
        
        session = get_snowflake_session()
        
        # Generate mapping ID
        mapping_id = f"{entity_id}_{vendor_name.replace(' ', '_').replace(',', '').replace('.', '').upper()}"
        
        # Insert new mapping
        session.sql(f"""
            INSERT INTO ENTITY_VENDOR_MAPPING (
                MAPPING_ID, ENTITY_ID, VENDOR_NAME, ENTITY_VENDOR_CODE, STATUS
            ) VALUES (
                '{mapping_id}',
                '{entity_id}',
                '{vendor_name.replace("'", "''")}',
                '{vendor_code.replace("'", "''")}',
                '{mapping_data.get('status', 'Active')}'
            )
        """).collect()
        
        return {"success": True, "message": f"Vendor mapping added successfully"}
        
    except Exception as e:
        print(f"Error adding vendor mapping to Snowflake: {e}")
        return {"success": False, "error": f"Database error: {str(e)}"}

def update_vendor_mapping_in_snowflake(mapping_id, mapping_data):
    """Update vendor mapping in Snowflake"""
    try:
        # Validate required field
        if not mapping_data.get('entity_vendor_code'):
            return {"success": False, "error": "Vendor code is required"}
        
        # Clean input
        vendor_code = mapping_data['entity_vendor_code'].strip()
        
        session = get_snowflake_session()
        
        # Update mapping
        session.sql(f"""
            UPDATE ENTITY_VENDOR_MAPPING SET 
                ENTITY_VENDOR_CODE = '{vendor_code.replace("'", "''")}',
                STATUS = '{mapping_data.get('status', 'Active')}',
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE MAPPING_ID = '{mapping_id}'
        """).collect()
        
        return {"success": True, "message": "Vendor mapping updated successfully"}
        
    except Exception as e:
        print(f"Error updating vendor mapping in Snowflake: {e}")
        return {"success": False, "error": f"Database error: {str(e)}"}

def delete_vendor_mapping_from_snowflake(mapping_id):
    """Delete vendor mapping from Snowflake"""
    try:
        session = get_snowflake_session()
        
        session.sql(f"""
            DELETE FROM ENTITY_VENDOR_MAPPING 
            WHERE MAPPING_ID = '{mapping_id}'
        """).collect()
        
        return {"success": True, "message": "Vendor mapping deleted successfully"}
        
    except Exception as e:
        print(f"Error deleting vendor mapping from Snowflake: {e}")
        return {"success": False, "error": f"Database error: {str(e)}"}

def check_mapping_duplicates(entity_id, vendor_name, exclude_mapping_id=None):
    """Check for duplicate entity-vendor combinations"""
    try:
        session = get_snowflake_session()
        
        query = f"""
            SELECT COUNT(*) FROM ENTITY_VENDOR_MAPPING 
            WHERE ENTITY_ID = '{entity_id}' AND VENDOR_NAME = '{vendor_name.replace("'", "''")}'
        """
        if exclude_mapping_id:
            query += f" AND MAPPING_ID != '{exclude_mapping_id}'"
        
        result = session.sql(query).collect()
        if result[0][0] > 0:
            return {"is_duplicate": True, "error": f"Mapping for this entity-vendor combination already exists"}
        
        return {"is_duplicate": False, "error": None}
        
    except Exception as e:
        print(f"Error checking mapping duplicates: {e}")
        return {"is_duplicate": False, "error": None}

# Enhanced duplicate checking functions
def check_entity_duplicates(entity_data, exclude_id=None):
    """
    Comprehensive duplicate checking for entities
    Returns: {"is_duplicate": True/False, "error": "message"}
    """
    try:
        session = get_snowflake_session()
        
        # Check for duplicate entity ID/code
        code_query = f"""
            SELECT COUNT(*) FROM ENTITY_CATALOG 
            WHERE ENTITY_ID = '{entity_data['code']}'
        """
        if exclude_id:
            code_query += f" AND ENTITY_ID != '{exclude_id}'"
        
        code_result = session.sql(code_query).collect()
        if code_result[0][0] > 0:
            return {"is_duplicate": True, "error": f"Entity code '{entity_data['code']}' already exists"}
        
        # Check for duplicate entity name
        name_query = f"""
            SELECT COUNT(*) FROM ENTITY_CATALOG 
            WHERE UPPER(ENTITY_NAME) = UPPER('{entity_data['name'].replace("'", "''")}')
        """
        if exclude_id:
            name_query += f" AND ENTITY_ID != '{exclude_id}'"
            
        name_result = session.sql(name_query).collect()
        if name_result[0][0] > 0:
            return {"is_duplicate": True, "error": f"Entity name '{entity_data['name']}' already exists"}
        
        # Check for duplicate email (if provided)
        if entity_data.get('email') and entity_data['email'].strip():
            email_query = f"""
                SELECT COUNT(*) FROM ENTITY_CATALOG 
                WHERE UPPER(EMAIL) = UPPER('{entity_data['email']}')
            """
            if exclude_id:
                email_query += f" AND ENTITY_ID != '{exclude_id}'"
                
            email_result = session.sql(email_query).collect()
            if email_result[0][0] > 0:
                return {"is_duplicate": True, "error": f"Email '{entity_data['email']}' is already in use"}
        
        return {"is_duplicate": False, "error": None}
        
    except Exception as e:
        print(f"Error checking entity duplicates: {e}")
        return {"is_duplicate": False, "error": None}

# Snowflake operations for entities (CURRENCY REMOVED)
def get_entities_from_snowflake():
    """Get all entities from Snowflake ENTITY_CATALOG table"""
    try:
        session = get_snowflake_session()
        
        result = session.sql("""
            SELECT ENTITY_ID, ENTITY_NAME, ENTITY_TYPE, STATUS, ADDRESS,
                   CONTACT_PERSON, EMAIL, PHONE, CREATED_AT
            FROM ENTITY_CATALOG 
            ORDER BY ENTITY_NAME
        """).collect()
        
        entities = []
        for row in result:
            entities.append({
                "entity_id": row[0],
                "name": row[1],
                "type": row[2],
                "status": row[3],
                "address": row[4],
                "contact": row[5],
                "email": row[6],
                "phone": row[7],
                "created_at": str(row[8]) if row[8] else None
            })
        
        return entities
        
    except Exception as e:
        print(f"Error getting entities from Snowflake: {e}")
        return []

def add_entity_to_snowflake(entity_data):
    """Add new entity to Snowflake ENTITY_CATALOG table (NO CURRENCY)"""
    try:
        # Validate required fields
        if not entity_data.get('code') or not entity_data.get('name'):
            return {"success": False, "error": "Entity code and name are required"}
        
        # Clean and sanitize input
        entity_data['code'] = entity_data['code'].strip().upper()
        entity_data['name'] = entity_data['name'].strip()
        
        # Check for duplicates
        duplicate_check = check_entity_duplicates(entity_data)
        if duplicate_check["is_duplicate"]:
            return {"success": False, "error": duplicate_check["error"]}
        
        session = get_snowflake_session()
        
        # Insert new entity (NO CURRENCY FIELD)
        session.sql(f"""
            INSERT INTO ENTITY_CATALOG (
                ENTITY_ID, ENTITY_NAME, ENTITY_TYPE, STATUS, ADDRESS,
                CONTACT_PERSON, EMAIL, PHONE
            ) VALUES (
                '{entity_data['code']}',
                '{entity_data['name'].replace("'", "''")}',
                '{entity_data.get('type', '')}',
                '{entity_data.get('status', 'Active')}',
                '{entity_data.get('address', '').replace("'", "''")}',
                '{entity_data.get('contact', '').replace("'", "''")}',
                '{entity_data.get('email', '')}',
                '{entity_data.get('phone', '')}'
            )
        """).collect()
        
        return {"success": True, "message": f"Entity '{entity_data['name']}' added successfully"}
        
    except Exception as e:
        print(f"Error adding entity to Snowflake: {e}")
        return {"success": False, "error": f"Database error: {str(e)}"}

def update_entity_in_snowflake(entity_id, entity_data):
    """Update entity in Snowflake ENTITY_CATALOG table (NO CURRENCY)"""
    try:
        # Validate required fields
        if not entity_data.get('name'):
            return {"success": False, "error": "Entity name is required"}
        
        # Clean and sanitize input
        entity_data['name'] = entity_data['name'].strip()
        
        # Check for duplicates (excluding current record)
        duplicate_check = check_entity_duplicates(entity_data, exclude_id=entity_id)
        if duplicate_check["is_duplicate"]:
            return {"success": False, "error": duplicate_check["error"]}
        
        session = get_snowflake_session()
        
        # Update entity (NO CURRENCY FIELD)
        session.sql(f"""
            UPDATE ENTITY_CATALOG SET 
                ENTITY_NAME = '{entity_data['name'].replace("'", "''")}',
                ENTITY_TYPE = '{entity_data.get('type', '')}',
                STATUS = '{entity_data.get('status', 'Active')}',
                ADDRESS = '{entity_data.get('address', '').replace("'", "''")}',
                CONTACT_PERSON = '{entity_data.get('contact', '').replace("'", "''")}',
                EMAIL = '{entity_data.get('email', '')}',
                PHONE = '{entity_data.get('phone', '')}',
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE ENTITY_ID = '{entity_id}'
        """).collect()
        
        return {"success": True, "message": f"Entity '{entity_data['name']}' updated successfully"}
        
    except Exception as e:
        print(f"Error updating entity in Snowflake: {e}")
        return {"success": False, "error": f"Database error: {str(e)}"}

def delete_entity_from_snowflake(entity_id):
    """Delete entity from Snowflake ENTITY_CATALOG table"""
    try:
        session = get_snowflake_session()
        
        session.sql(f"""
            DELETE FROM ENTITY_CATALOG 
            WHERE ENTITY_ID = '{entity_id}'
        """).collect()
        
        return {"success": True, "message": "Entity deleted successfully"}
        
    except Exception as e:
        print(f"Error deleting entity from Snowflake: {e}")
        return {"success": False, "error": f"Database error: {str(e)}"}

# Snowflake operations for vendors (NO VENDOR_ID FIELD)
def get_vendors_from_snowflake():
    """Get all vendors from Snowflake VENDOR_CATALOG table"""
    try:
        session = get_snowflake_session()
        
        result = session.sql("""
            SELECT VENDOR_NAME, VENDOR_TYPE, STATUS, ADDRESS,
                   CONTACT_PERSON, EMAIL, PHONE, CURRENCY, CREATED_AT
            FROM VENDOR_CATALOG 
            ORDER BY VENDOR_NAME
        """).collect()
        
        vendors = []
        for row in result:
            vendors.append({
                "name": row[0],
                "type": row[1],
                "status": row[2],
                "address": row[3],
                "contact": row[4],
                "email": row[5],
                "phone": row[6],
                "currency": row[7],
                "created_at": str(row[8]) if row[8] else None
            })
        
        return vendors
        
    except Exception as e:
        print(f"Error getting vendors from Snowflake: {e}")
        return []

def add_vendor_to_snowflake(vendor_data):
    """Add new vendor to Snowflake VENDOR_CATALOG table (NO VENDOR_ID FIELD)"""
    try:
        # Validate required fields - only name is required
        if not vendor_data.get('name'):
            return {"success": False, "error": "Vendor name is required"}
        
        # Clean and sanitize input
        vendor_data['name'] = vendor_data['name'].strip()
        
        # Check for duplicate vendor name only
        session = get_snowflake_session()
        name_query = f"""
            SELECT COUNT(*) FROM VENDOR_CATALOG 
            WHERE UPPER(VENDOR_NAME) = UPPER('{vendor_data['name'].replace("'", "''")}')
        """
        name_result = session.sql(name_query).collect()
        if name_result[0][0] > 0:
            return {"success": False, "error": f"Vendor name '{vendor_data['name']}' already exists"}
        
        # Check for duplicate email (if provided)
        if vendor_data.get('email') and vendor_data['email'].strip():
            email_query = f"""
                SELECT COUNT(*) FROM VENDOR_CATALOG 
                WHERE UPPER(EMAIL) = UPPER('{vendor_data['email']}')
            """
            email_result = session.sql(email_query).collect()
            if email_result[0][0] > 0:
                return {"success": False, "error": f"Email '{vendor_data['email']}' is already in use"}
        
        # Insert new vendor (NO VENDOR_ID FIELD)
        session.sql(f"""
            INSERT INTO VENDOR_CATALOG (
                VENDOR_NAME, VENDOR_TYPE, STATUS, ADDRESS,
                CONTACT_PERSON, EMAIL, PHONE, CURRENCY
            ) VALUES (
                '{vendor_data['name'].replace("'", "''")}',
                '{vendor_data.get('type', '')}',
                '{vendor_data.get('status', 'Active')}',
                '{vendor_data.get('address', '').replace("'", "''")}',
                '{vendor_data.get('contact', '').replace("'", "''")}',
                '{vendor_data.get('email', '')}',
                '{vendor_data.get('phone', '')}',
                '{vendor_data.get('currency', '')}'
            )
        """).collect()
        
        return {"success": True, "message": f"Vendor '{vendor_data['name']}' added successfully"}
        
    except Exception as e:
        print(f"Error adding vendor to Snowflake: {e}")
        return {"success": False, "error": f"Database error: {str(e)}"}

def update_vendor_in_snowflake(vendor_name, vendor_data):
    """Update vendor in Snowflake VENDOR_CATALOG table (NO VENDOR_ID FIELD)"""
    try:
        # Validate required fields
        if not vendor_data.get('name'):
            return {"success": False, "error": "Vendor name is required"}
        
        # Clean and sanitize input
        vendor_data['name'] = vendor_data['name'].strip()
        
        session = get_snowflake_session()
        
        # Check for duplicates (excluding current record)
        if vendor_name != vendor_data['name']:  # Only check if name is changing
            name_query = f"""
                SELECT COUNT(*) FROM VENDOR_CATALOG 
                WHERE UPPER(VENDOR_NAME) = UPPER('{vendor_data['name'].replace("'", "''")}')
            """
            name_result = session.sql(name_query).collect()
            if name_result[0][0] > 0:
                return {"success": False, "error": f"Vendor name '{vendor_data['name']}' already exists"}
        
        # Update vendor (NO VENDOR_ID FIELD)
        session.sql(f"""
            UPDATE VENDOR_CATALOG SET 
                VENDOR_NAME = '{vendor_data['name'].replace("'", "''")}',
                VENDOR_TYPE = '{vendor_data.get('type', '')}',
                STATUS = '{vendor_data.get('status', 'Active')}',
                ADDRESS = '{vendor_data.get('address', '').replace("'", "''")}',
                CONTACT_PERSON = '{vendor_data.get('contact', '').replace("'", "''")}',
                EMAIL = '{vendor_data.get('email', '')}',
                PHONE = '{vendor_data.get('phone', '')}',
                CURRENCY = '{vendor_data.get('currency', '')}',
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE VENDOR_NAME = '{vendor_name}'
        """).collect()
        
        return {"success": True, "message": f"Vendor '{vendor_data['name']}' updated successfully"}
        
    except Exception as e:
        print(f"Error updating vendor in Snowflake: {e}")
        return {"success": False, "error": f"Database error: {str(e)}"}

def delete_vendor_from_snowflake(vendor_name):
    """Delete vendor from Snowflake VENDOR_CATALOG table (NO VENDOR_ID FIELD)"""
    try:
        session = get_snowflake_session()
        
        session.sql(f"""
            DELETE FROM VENDOR_CATALOG 
            WHERE VENDOR_NAME = '{vendor_name}'
        """).collect()
        
        return {"success": True, "message": "Vendor deleted successfully"}
        
    except Exception as e:
        print(f"Error deleting vendor from Snowflake: {e}")
        return {"success": False, "error": f"Database error: {str(e)}"}

# Helper function to initialize tables if they don't exist
def init_snowflake_tables():
    """Initialize Snowflake catalog tables if they don't exist"""
    try:
        session = get_snowflake_session()
        
        # Create ENTITY_CATALOG table (NO CURRENCY)
        session.sql("""
            CREATE TABLE IF NOT EXISTS ENTITY_CATALOG (
                ENTITY_ID VARCHAR(50) NOT NULL PRIMARY KEY,
                ENTITY_NAME VARCHAR(255) NOT NULL,
                ENTITY_TYPE VARCHAR(50),
                STATUS VARCHAR(20) DEFAULT 'Active',
                ADDRESS TEXT,
                CONTACT_PERSON VARCHAR(255),
                EMAIL VARCHAR(255),
                PHONE VARCHAR(50),
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """).collect()
        
        # Create VENDOR_CATALOG table (NO VENDOR_ID)
        session.sql("""
            CREATE TABLE IF NOT EXISTS VENDOR_CATALOG (
                VENDOR_NAME VARCHAR(255) NOT NULL PRIMARY KEY,
                VENDOR_TYPE VARCHAR(50),
                STATUS VARCHAR(20) DEFAULT 'Active',
                ADDRESS TEXT,
                CONTACT_PERSON VARCHAR(255),
                EMAIL VARCHAR(255),
                PHONE VARCHAR(50),
                CURRENCY VARCHAR(3),
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """).collect()
        
        # Create ENTITY_VENDOR_MAPPING table (USING VENDOR_NAME INSTEAD OF VENDOR_ID)
        session.sql("""
            CREATE TABLE IF NOT EXISTS ENTITY_VENDOR_MAPPING (
                MAPPING_ID VARCHAR(50) PRIMARY KEY,
                ENTITY_ID VARCHAR(50) NOT NULL,
                VENDOR_NAME VARCHAR(255) NOT NULL,
                ENTITY_VENDOR_CODE VARCHAR(50) NOT NULL,
                STATUS VARCHAR(20) DEFAULT 'Active',
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                UNIQUE (ENTITY_ID, VENDOR_NAME)
            )
        """).collect()
        
        print("✅ Snowflake catalog tables initialized successfully")
        
    except Exception as e:
        print(f"❌ Error initializing Snowflake tables: {e}")