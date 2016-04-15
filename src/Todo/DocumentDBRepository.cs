namespace Todo
{
	using System;
	using System.Collections.Generic;
	using System.Configuration;
	using System.Linq;
	using Microsoft.Azure.Documents;
	using Microsoft.Azure.Documents.Client;
	using Microsoft.Azure.Documents.Linq;
	using Models;
	using System.Threading.Tasks;
	using Newtonsoft.Json;
	public static class DocumentDBRepository
	{
		private static string databaseId;
		private static string collectionId;
		private static Database database;
		private static DocumentCollection collection;
		private static DocumentClient client;

		private static string DatabaseId
		{
			get
			{
				if (string.IsNullOrEmpty(databaseId))
				{
					databaseId = ConfigurationManager.AppSettings["database"];
				}

				return databaseId;
			}
		}

		private static string CollectionId
		{
			get
			{
				if (string.IsNullOrEmpty(collectionId))
				{
					collectionId = ConfigurationManager.AppSettings["collection"];
				}

				return collectionId;
			}
		}

		private static Database Database
		{
			get
			{
				if (database == null)
				{
					database = ReadOrCreateDatabase();
				}

				return database;
			}
		}

		private static DocumentCollection Collection
		{
			get
			{
				if (collection == null)
				{
					collection = ReadOrCreateCollection(Database.SelfLink);
				}

				return collection;
			}
		}

		private static DocumentClient Client
		{
			get
			{
				if (client == null)
				{
					string endpoint = ConfigurationManager.AppSettings["endpoint"];
					string authKey = ConfigurationManager.AppSettings["authKey"];
					Uri endpointUri = new Uri(endpoint);
					client = new DocumentClient(endpointUri, authKey);
				}

				return client;
			}
		}

		private static DocumentCollection ReadOrCreateCollection(string databaseLink)
		{
			var col = Client.CreateDocumentCollectionQuery(databaseLink)
									.Where(c => c.Id == CollectionId)
									.AsEnumerable()
									.FirstOrDefault();

			if (col == null)
			{
				col = Client.CreateDocumentCollectionAsync(databaseLink, new DocumentCollection { Id = CollectionId }).Result;
			}

			return col;
		}

		private static Database ReadOrCreateDatabase()
		{
			var db = Client.CreateDatabaseQuery()
								 .Where(d => d.Id == DatabaseId)
								 .AsEnumerable()
								 .FirstOrDefault();

			if (db == null)
			{
				db = Client.CreateDatabaseAsync(new Database { Id = DatabaseId }).Result;
			}

			return db;
		}


		public static List<Item> GetIncompleteItems()
		{
			return Client.CreateDocumentQuery<Item>(Collection.DocumentsLink)
					   .Where(d => !d.Completed)
					   .AsEnumerable()
					   .ToList<Item>();
		}

		public static async Task<Document> CreateItemAsync(Item item)
		{
			return await Client.CreateDocumentAsync(Collection.SelfLink, item);
		}

		public static Item GetItem(string id)
		{
			return Client.CreateDocumentQuery<Item>(Collection.DocumentsLink)
							.Where(d => d.Id == id)
							.AsEnumerable()
							.FirstOrDefault();
		}

		public static Document GetDocument(string id)
		{
			return Client.CreateDocumentQuery(Collection.DocumentsLink)
						  .Where(d => d.Id == id)
						  .AsEnumerable()
						  .FirstOrDefault();
		}

		public static async Task<Document> UpdateItemAsync(Item item)
		{
			Document doc = GetDocument(item.Id);
			return await Client.ReplaceDocumentAsync(doc.SelfLink, item);
		}

		public static async Task DeleteItemAsync(string id)
		{
			Document doc = GetDocument(id);
			await Client.DeleteDocumentAsync(doc.SelfLink);
		}

		public static async Task<StoredProcedure> GetMarkAllStoredProcedure()
		{
			string sprocId = "MarkAll";

			StoredProcedure sproc = await GetStoredProcedureAsync(sprocId);

			if (sproc != null)
			{
				return sproc;
			}

			var markAllsSproc = new StoredProcedure
			{
				Id = sprocId,
				Body = @"function markAll(docs) {
										var collection = getContext().getCollection();
										var collectionLink = collection.getSelfLink();

										// The count of docs, also used as current doc index.
										var count = 0;

										// Validate input.
										if (!docs) throw new Error(""The array is undefined or null."");

										var docsLength = docs.length;
										if (docsLength == 0) {
												getContext().getResponse().setBody(0);
										}

										// Call the create API to create a document.
										tryMark(docs[count], callback);

										// Note that there are 2 exit conditions:
										// 1) The createDocument request was not accepted. 
										//    In this case the callback will not be called, we just call setBody and we are done.
										// 2) The callback was called docs.length times.
										//    In this case all documents were created and we don’t need to call tryCreate anymore. Just call setBody and we are done.
										function tryMark(doc, callback) {
												doc.isComplete = true; 
												var isAccepted = collection.replaceDocument(doc._self, doc, callback);

												// If the save was accepted, callback will be called.
												// Otherwise report current count back to the client, 
												// which will call the script again with remaining set of docs.
												if (!isAccepted) getContext().getResponse().setBody(count);
										}

										// This is called when collection.createDocument is done in order to process the result.
										function callback(err, doc, options) {
												if (err) throw err;

												// One more document has been updated, increment the count.
												count++;

												if (count >= docsLength) {
														// If we created all documents, we are done. Just set the response.
														getContext().getResponse().setBody(count);
												} else {
														// Create next document.
														tryMark(docs[count], callback);
												}
										}
								}"
			};

			StoredProcedure createdStoredProcedure = await client.CreateStoredProcedureAsync(collection.SelfLink, markAllsSproc);

			return createdStoredProcedure;
		}

		private static async Task<StoredProcedure> GetStoredProcedureAsync(string sprocId)
		{
			return await Task.Run(() => client.CreateStoredProcedureQuery(collection.SelfLink).Where(s => s.Id == sprocId).AsEnumerable().FirstOrDefault());
		}

		public static async Task MarkAllAsCompleted()
		{
			StoredProcedure sp = await GetMarkAllStoredProcedure();

			var items = Client.CreateDocumentQuery<Document>(Collection.SelfLink, "SELECT * from i where i.isComplete = false")
                       .AsEnumerable().ToList();

            dynamic[] args = new[] { JsonConvert.DeserializeObject<dynamic>(JsonConvert.SerializeObject(items)) };

			var updatedCount = await client.ExecuteStoredProcedureAsync<int>(sp.SelfLink, args);
		}
	}
}