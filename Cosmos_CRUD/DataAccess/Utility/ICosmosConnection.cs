using Microsoft.Azure.Documents.Client;
using System.Threading.Tasks;

namespace Cosmos_CRUD.DataAccess.Utility
{
   
    public interface ICosmosConnection
    {
        Task<DocumentClient> InitializeAsync(string collectionId);
    }
}
