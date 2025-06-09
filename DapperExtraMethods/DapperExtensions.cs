using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

#nullable enable

namespace DatabaseProvider
{
	public static class DapperExtensions
	{
		public static async Task<IEnumerable<TOne>> OneToManyAsync<TOne, TMany>
			(this IDbConnection connection, string sql, Func<TOne, object> getKey, Action<TOne, TMany> addMany,
			string splitOn = "Id", object? param = null, IDbTransaction? transaction = null)
		{
			var rows = new Dictionary<object, TOne>();

			await connection.QueryAsync<TOne, TMany, TOne>(sql,
			(oneRow, manyRow) =>
			{
				object key = getKey(oneRow);
				TOne? one = default;

				rows.TryGetValue(key, out one);
				if (one == null)
				{
					one = oneRow;
					rows[key] = one;
				}
				addMany(one, manyRow);

				return oneRow;
			},
			splitOn: splitOn,
			param: param,
			transaction: transaction);

			return rows.Select(x => x.Value);
		}

		public static async Task<IEnumerable<TReturn>> OneToManySelectAsync<TOne, TMany, TReturn>
			(this IDbConnection connection, string sql, Func<TOne, object> getKey,
			Func<TOne, IEnumerable<TMany>, TReturn> select,
			string splitOn = "Id", object? param = null, IDbTransaction? transaction = null)
		{
			var rows = new Dictionary<object, (TOne, ICollection<TMany>)>();

			await connection.QueryAsync<TOne, TMany, TOne>(sql,
			(oneRow, manyRow) =>
			{
				object key = getKey(oneRow);

				(TOne, ICollection<TMany>) tuple = default;
				rows.TryGetValue(key, out tuple);

				TOne? one = tuple.Item1;
				ICollection<TMany> many = tuple.Item2;

				if (one == null)
				{
					one = oneRow;
					many = new List<TMany>();
					rows[key] = (one, many);
				}
				many.Add(manyRow);

				return oneRow;
			},
			splitOn: splitOn,
			param: param,
			transaction: transaction);

			return rows.Select(x => select(x.Value.Item1, x.Value.Item2));
		}

		public static async Task<IEnumerable<TReturn>> OneToManySelectAsync<TOne, KOne, TMany, TReturn>
			(this IDbConnection connection, string sql, Func<TOne, KOne, object> getKey,
			Func<TOne, KOne, IEnumerable<TMany>, TReturn> select,
			string splitOn = "Id", object? param = null, IDbTransaction? transaction = null)
		{
			var rows = new Dictionary<object, (TOne, KOne, ICollection<TMany>)>();

			await connection.QueryAsync<TOne, KOne, TMany, TOne>(sql,
			(oneRow, oneRow2, manyRow) =>
			{
				object key = getKey(oneRow, oneRow2);

				(TOne, KOne, ICollection<TMany>) tuple = default;
				rows.TryGetValue(key, out tuple);

				TOne? one = tuple.Item1;
				KOne? one2 = tuple.Item2;
				ICollection<TMany> many = tuple.Item3;

				if (one == null)
				{
					one = oneRow;
					one2 = oneRow2;
					many = new List<TMany>();
					rows[key] = (one, one2, many);
				}
				many.Add(manyRow);

				return oneRow;
			},
			splitOn: splitOn,
			param: param,
			transaction: transaction);

			return rows.Select(x => select(x.Value.Item1, x.Value.Item2, x.Value.Item3));
		}

		public static async Task<TOne?> OneToManyFirstOrDefaultAsync<TOne, TMany>
			(this IDbConnection connection, string sql, Action<TOne, TMany> addMany, string splitOn = "Id", object? param = null,
			IDbTransaction? transaction = null)
		{
			TOne? one = default;

			await connection.QueryAsync<TOne, TMany, TOne>(sql,
			(oneRow, manyRow) =>
			{
				if (one == null)
					one = oneRow;

				addMany(one, manyRow);

				return oneRow;
			},
			splitOn: splitOn,
			param: param,
			transaction: transaction);

			return one;
		}

		public static async Task<IEnumerable<TParent>> QueryOneToOneAsync<TParent, TChild>(this IDbConnection connection, string sql,
			Action<TParent, TChild> addChild, string splitOn = "Id", object? param = null, IDbTransaction? transaction = null)
		{
			return await connection.QueryAsync<TParent, TChild, TParent>(sql,
			(parent, child) =>
			{
				addChild(parent, child);
				return parent;
			},
			splitOn: splitOn,
			param: param,
			transaction: transaction);
		}

		public static async Task<IEnumerable<Level1>> QueryOneToManyNested<Level1, Level2, Level3>(this IDbConnection connection,
			string sql, Action<Level1, Level2> addLevel2, Action<Level2, Level3> addLevel3, Func<Level1, object> getKeyLevel1,
			Func<Level2, object> getKeyLevel2, string splitOn = "Id", object? param = null, IDbTransaction? transaction = null)
		{
			var rows = new Dictionary<object, Level1>();
			var rowsLevel2 = new Dictionary<(object, object), Level2>();

			await connection.QueryAsync<Level1, Level2, Level3, Level1>(sql,
			(rowLevel1, rowLevel2, rowLevel3) =>
			{
				object keyLevel1 = getKeyLevel1(rowLevel1);
				Level1? level1 = default;

				rows.TryGetValue(keyLevel1, out level1);

				if (level1 == null)
				{
					level1 = rowLevel1;
					rows[keyLevel1] = level1;
				}

				object keyLevel2 = getKeyLevel2(rowLevel2);
				Level2? level2 = default;

				rowsLevel2.TryGetValue((keyLevel1, keyLevel2), out level2);

				if (level2 == null)
				{
					level2 = rowLevel2;
					rowsLevel2[(keyLevel1, keyLevel2)] = level2;
				}

				addLevel3(level2, rowLevel3);

				return rowLevel1;
			},
			splitOn: splitOn,
			param: param,
			transaction: transaction);

			foreach (KeyValuePair<(object, object), Level2> kv in rowsLevel2)
			{
				object keyLevel1 = kv.Key.Item1;
				Level2 level2 = kv.Value;

				bool hasParent = rows.TryGetValue(keyLevel1, out Level1? level1);

				if (hasParent && level1 != null)
					addLevel2(level1, level2);
			}

			return rows.Select(x => x.Value);
		}
	}
}
