package cloudComputing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateGlobalSecondaryIndexAction;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

public class DynamoDBDataAccess {
	BasicAWSCredentials credentials = new BasicAWSCredentials("AKIAI2RJ6PBMW2VYKAOA", "vxUT7QFNj0Yioz7KMvuKMxYXXQNpjrGOyhx1qbxQ");
	AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(Regions.US_EAST_1).build();
	
	DynamoDB dynamoDB = new DynamoDB(client);
	JSONParser jsonParser = new JSONParser();
	
	public String getFieldValueDynamoDB(String tableName, String primaryKey, String primaryKeyValue, String returnField) throws ParseException{
		
		Table table = dynamoDB.getTable(tableName);
		String returnValue = "";
		//Build the GeTItem Specification
		GetItemSpec spec = new GetItemSpec().withPrimaryKey(primaryKey,primaryKeyValue).withProjectionExpression(returnField).withConsistentRead(true);
		Item item = table.getItem(spec);
		
		Object obj = jsonParser.parse(item.toJSONPretty());
		JSONObject jObject = (JSONObject) obj;
		returnValue = (String) jObject.get(returnField);
		
		return returnValue;
	}
	
	public void listTables() {
	
		ListTablesResult tableList = null;
		tableList = client.listTables();

		List<String> tableNames = tableList.getTableNames();

		if (tableNames.size() > 0) {
			for (String curTableName : tableNames) {
				System.out.format("* %s\n", curTableName);
			}
		} else {
			System.out.println("No tables foundd");
		}
	}

	public boolean updateColumnDynamoDB(String tableName, String primaryKey, String primaryKeyValue,
			String updateColumn, String upddateColumnValue) {
		try {
			Table table = dynamoDB.getTable(tableName);
			// Building itemUpdate Specification
			UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey(primaryKey, primaryKeyValue)
					.withReturnValues(ReturnValue.ALL_NEW).withUpdateExpression("set #columnName = :columnValue")
					.withNameMap(new NameMap().with("#columnName", updateColumn))
					.withValueMap(new ValueMap().with(":columnValue:", upddateColumnValue));

			table.updateItem(updateItemSpec);

			return true;
		} catch (Exception e) {
			System.out.println("Error in updating table " + tableName + " ::: " + e.getMessage());
			return false;
		}
	}
	
	
	public boolean insertKeyValue(String tableName, String primaryKey, String primaryKeyValue, String updateColumn,String newKey, String newValue ){
		
		//Configuration to connect to DynamoDB
		Table table = dynamoDB.getTable(tableName);
		boolean insertAppendStatus = false;
		try{
		//Updates when map is already exist in the table
		UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey(primaryKey,primaryKeyValue).withReturnValues(ReturnValue.ALL_NEW).
				withUpdateExpression("set #columnName." + newKey + " = :columnValue").
				withNameMap(new NameMap().with("#columnName", updateColumn)).
				withValueMap(new ValueMap().with(":columnValue", newValue)).withConditionExpression("attribute_exists("+ updateColumn +")");
		
		table.updateItem(updateItemSpec);
		insertAppendStatus = true;
		//Add map column when it's not exist in the table
		}catch (ConditionalCheckFailedException e) {
			HashMap<String, String> map =  new HashMap<>();
			map.put(newKey, newValue);
			UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey(primaryKey,primaryKeyValue).withReturnValues(ReturnValue.ALL_NEW).
				withUpdateExpression("set #columnName = :m").
					withNameMap(new NameMap().with("#columnName", updateColumn))
					.withValueMap(new ValueMap().withMap(":m", map));
			
			table.updateItem(updateItemSpec);
			insertAppendStatus = true;
		} catch(Exception e){
			e.printStackTrace();
		}
		return insertAppendStatus;
	}
	
	public boolean appendToList(String tableName, String primaryKey, String primaryKeyValue, String updateColumn, ArrayList<String> listElement){
		
		try{
			Table table = dynamoDB.getTable(tableName);
			System.out.println(String.format("set %s = list_append(%s,:newList)", updateColumn,updateColumn));
			
			UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey(primaryKey,primaryKeyValue).withReturnValues(ReturnValue.ALL_NEW).
					withUpdateExpression(String.format("set %s = list_append(%s,:newList)", updateColumn,updateColumn)).
					withValueMap(new ValueMap().withList(":newList", listElement));
			//If you want to append at the beginning of the list then use set %s = list_append(:newList,%s) update expression
			table.updateItem(updateItemSpec);
			return true;
			}catch (Exception e) {
				e.printStackTrace();
				return false;
			}
	}
	
	public boolean addOrAppendtoList(String tableName, String primaryKey, String primaryKeyValue, String column,
			ArrayList<String> listElement) {
		try {
			Table table = dynamoDB.getTable(tableName);

			Item item = new Item().withPrimaryKey(primaryKey, primaryKeyValue).withList(column, listElement);

			PutItemSpec putItemSpec = new PutItemSpec().withItem(item)
					.withConditionExpression("attribute_not_exists(" + primaryKey + ")");
			table.putItem(putItemSpec);

			return true;
		} catch (ConditionalCheckFailedException e) {
			appendToList(tableName, primaryKey, primaryKeyValue, column, listElement);
			return true;
		} catch (Exception e) {
			System.out.println("Erro :::" + e.getMessage());
			return false;
		}
	}
	
	public String[] getFieldDynamoDB(String tableName, String primaryKey, String primaryKeyValue, String projectionColumns){
		Table table = dynamoDB.getTable(tableName);
		JSONParser jsonParser = new JSONParser();
		
		String[] columnsArray = projectionColumns.split(",");
		NameMap reservedKeyMap = new NameMap();
		String columns = null;
		int arrayIndex = 0;
		String[] outPutArray = new String[columnsArray.length];
		/*The below for loop is to avoid reserved keyword exception , this loop will add # before every column to let dynamoDB know that it's a reserved keyword and
		 value of them will be in reservedKeyMap
		 If you are 100% sure you will not have any reserved keyword then skip this part and simple pass the input string projectionColumns
		 */
		for (int i = 0; i < columnsArray.length; i++) {
			if(i ==0){
				columns = "#" + columnsArray[i];
			}
			else {
				columns += ",#" + columnsArray[i];
			}
			reservedKeyMap.put("#" + columnsArray[i], columnsArray[i]);
		}
		
		GetItemSpec getSpec = new GetItemSpec().withPrimaryKey(primaryKey, primaryKeyValue).withProjectionExpression(columns).withConsistentRead(true);
		getSpec.withNameMap(reservedKeyMap);
		
		Item item = table.getItem(getSpec);
		try {
			Object obj = jsonParser.parse(item.toJSONPretty());
			JSONObject jObject = (JSONObject) obj;
			for (String columnKey : columnsArray) {
				outPutArray[arrayIndex] = jObject.get(columnKey).toString();
				System.out.println(outPutArray[arrayIndex]);
				arrayIndex++;
			}
			
		} catch (ParseException e) {
			System.out.println(e.getMessage());
		}
		
		return outPutArray;
	}
	
	public String getValueFromDynamoDBIindex(String tableName, String indexName, String indexKey,
			String indexValue, String projectioColumns) {
		Index index = dynamoDB.getTable(tableName).getIndex(indexName);
		String returnedValue = null;

		QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("#indexKey = :value1")
				.withNameMap(new NameMap().with("#indexKey", indexKey))
				.withValueMap(new ValueMap().withString(":value1", indexValue))
				.withProjectionExpression(projectioColumns);
		/*
		 * When you extract data from Index there is a chance that you get more
		 * than one record for a particular indexKey, the below iterator is
		 * being used for that
		 */
		ItemCollection<QueryOutcome> items = index.query(querySpec);
		Iterator<Item> itemIterator = items.iterator();

		while (itemIterator.hasNext()) {

			try {
				Object obj = jsonParser.parse(itemIterator.next().toJSONPretty());
				JSONObject jsonObject = (JSONObject) obj;
				returnedValue = jsonObject.get(projectioColumns).toString();
				System.out.println(returnedValue);
			} catch (ParseException e) {
				System.out.println(e.getMessage());
			}

		}

		return returnedValue;
	}
	
	public boolean createDynamoDBTable(String tableName, String primaryKey, String primaryKeyType) {
		// Create key Schema
		ArrayList<KeySchemaElement> listKeySchema = new ArrayList<KeySchemaElement>();
		KeySchemaElement kk = new KeySchemaElement(primaryKey, KeyType.HASH);
		// Add the columns in attribute definition
		ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
		attributeDefinitions
				.add(new AttributeDefinition().withAttributeName(primaryKey).withAttributeType(primaryKeyType));
		listKeySchema.add(kk);
		// Create table request
		CreateTableRequest req = new CreateTableRequest().withTableName(tableName).withKeySchema(listKeySchema)
				.withAttributeDefinitions(attributeDefinitions).withProvisionedThroughput(
						new ProvisionedThroughput().withReadCapacityUnits(5L).withWriteCapacityUnits(5L));
		try {
			dynamoDB.createTable(req).waitForActive();
			return true;
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
		}
		return false;
	}
	
	public boolean createIndexDynamoDB(String tableName, String indexName, String indexColumn, String columnDataType){
		Table table = dynamoDB.getTable(tableName);
		//Define key schema
		ArrayList<KeySchemaElement> listKeySchema = new ArrayList<KeySchemaElement>();
		KeySchemaElement keySchema = new KeySchemaElement(indexColumn, KeyType.HASH);
		listKeySchema.add(keySchema);
		//Define the hash key definitions
		AttributeDefinition attributeDefinitions = new AttributeDefinition().withAttributeName(indexColumn).withAttributeType(columnDataType);
		//Create Index
		try {
			table.createGSI(new CreateGlobalSecondaryIndexAction().withIndexName(indexName).withKeySchema(listKeySchema).
					withProjection(new Projection().withProjectionType(ProjectionType.ALL)).withProvisionedThroughput(new ProvisionedThroughput().
							withReadCapacityUnits(2L).withWriteCapacityUnits(2L)),attributeDefinitions).waitForActive();
			return true;
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
		}
		
		return false;
	}
	
	public boolean dropIndex(String tableName, String indexName){
		
		Index index = dynamoDB.getTable(tableName).getIndex(indexName);
		index.deleteGSI();
		try {
			index.waitForDelete();
			return true;
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
		}
		return false;
	}
	
	public boolean deleteItem(String tableName, String primaryKey, String primaryKeyValue){
		
		Table table = dynamoDB.getTable(tableName);
		table.deleteItem(new PrimaryKey().addComponent(primaryKey, primaryKeyValue));
		return true;
	}
	
	public boolean deleteTable(String tableName) {

		Table table = dynamoDB.getTable(tableName);
		table.delete();
		try {
			table.waitForDelete();
			return true;
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
		}

		return false;
	}
	
	public JSONObject getItemDynamoDB(String tableName, String primaryKey,
			String primaryKeyValue) {
		Table index = dynamoDB.getTable(tableName);

		QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("#primaryKey = :value1")
				.withNameMap(new NameMap().with("#primaryKey", primaryKey))
				.withValueMap(new ValueMap().withString(":value1", primaryKeyValue));
		
		ItemCollection<QueryOutcome> items = index.query(querySpec);
		Iterator<Item> itemIterator = items.iterator();

				try {
					Object obj = jsonParser.parse(itemIterator.next().toJSONPretty());
					JSONObject jsonObject = (JSONObject) obj;
					return jsonObject;
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
				
	}
	
	@SuppressWarnings("unchecked")
	public JSONArray concatenateMultipleItem(){
		
		JSONArray jsonArray = new JSONArray() ;
		
		jsonArray.add(getItemDynamoDB("Dummy", "CLM_ID", "1"));
		jsonArray.add(getItemDynamoDB("Dummy", "CLM_ID", "2"));
		
		System.out.println(jsonArray);
		
		return jsonArray;
	}
}
