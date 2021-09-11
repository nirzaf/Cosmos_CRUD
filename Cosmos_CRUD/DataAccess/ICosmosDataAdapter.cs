using Cosmos_CRUD.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Cosmos_CRUD.DataAccess
{
    public interface ICosmosDataAdapter
    {
        Task<UserInfo> UpsertUserAsync(UserInfo user);

        Task<bool> CreateDatabase(string name);
        Task<bool> CreateCollection(string dbName, string name);
        Task<bool> CreateDocument(string dbName, string name, UserInfo userInfo);

        Task<bool> PlaceOrder(string dbName, string name, Order order);

        Task<dynamic> GetData(string dbName, string name);
        Task<UserInfo> DeleteUserAsync(string dbName, string name, string id);
    }
}