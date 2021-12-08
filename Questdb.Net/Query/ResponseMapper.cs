using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Newtonsoft.Json;
using NodaTime;
using Questdb.Net.Exceptions;

namespace Questdb.Net.Query
{
    public static class ResponseMapper
    {
        // Reflection results are cached for poco type property and attribute lookups as an optimization since
        // calls are invoked continuously for a given type and will not change over library lifetime
        private static readonly ConcurrentDictionary<Type, PropertyInfo[]> PropertyCache = new ConcurrentDictionary<Type, PropertyInfo[]>();
        private static readonly ConcurrentDictionary<PropertyInfo, Net.Column> AttributeCache = new ConcurrentDictionary<PropertyInfo, Net.Column>();

        /// <summary>
        /// Maps Record into custom POCO class.
        /// </summary>
        /// <param name="record">the record</param>
        /// <typeparam name="T">the POCO type</typeparam>
        /// <returns></returns>
        /// <exception cref="QuestdbException"></exception>
        public static List<T> ToPoco<T>(this QuestdbResponse data)
        {
            Arguments.CheckNotNull(data, "Data is required for mapping");
            if (!data.IsSuccessResponse)
                throw new QuestdbException(data.Error);

            List<T> result = new List<T>();
            if (data.DataSet == null || data.DataSet.Count == 0)
                return result;

            var type = typeof(T);
            Dictionary<PropertyInfo, int> propertyMap = data.Columns.MapCoulmsToProperties<T>();
            foreach (var recordValues in data.DataSet)
            {
                try
                {
                    var poco = (T)Activator.CreateInstance(type);

                    foreach (var property in propertyMap)
                    {
                        var attribute = AttributeCache.GetOrAdd(property.Key, _ =>
                        {
                            var attributes = property.Key.GetCustomAttributes(typeof(Net.Column), false);
                            return attributes.Length > 0 ? attributes[0] as Net.Column : null;
                        });

                        SetFieldValue(poco, property.Key, recordValues[property.Value]);
                    }

                    result.Add(poco);
                }
                catch (Exception e)
                {
                    throw new QuestdbException(e);
                }
            }
            return result;
        }

        internal static T ToPoco<T>(List<object> recordValues, List<TableColumn> columns)
        {
            Arguments.CheckNotNull(recordValues, "Data is required for mapping");
            Arguments.CheckNotNull(columns, "Columns is required for mapping");
            try
            {
                var type = typeof(T);
                Dictionary<PropertyInfo, int> propertyMap = columns.MapCoulmsToProperties<T>();
                var poco = (T)Activator.CreateInstance(type);

                foreach (var property in propertyMap)
                {
                    var attribute = AttributeCache.GetOrAdd(property.Key, _ =>
                    {
                        var attributes = property.Key.GetCustomAttributes(typeof(Net.Column), false);
                        return attributes.Length > 0 ? attributes[0] as Net.Column : null;
                    });

                    SetFieldValue(poco, property.Key, recordValues[property.Value]);
                }
                return poco;
            }
            catch (Exception e)
            {
                throw new QuestdbException(e);
            }
        }


        internal static T ToPoco<T>(List<object> recordValues, Dictionary<PropertyInfo, int> propertyMap)
        {
            Arguments.CheckNotNull(recordValues, "Data is required for mapping");
            Arguments.CheckNotNull(propertyMap, "PropertyMap is required for mapping");
            try
            {
                var type = typeof(T);
                var poco = (T)Activator.CreateInstance(type);

                foreach (var property in propertyMap)
                {
                    var attribute = AttributeCache.GetOrAdd(property.Key, _ =>
                    {
                        var attributes = property.Key.GetCustomAttributes(typeof(Net.Column), false);
                        return attributes.Length > 0 ? attributes[0] as Net.Column : null;
                    });

                    SetFieldValue(poco, property.Key, recordValues[property.Value]);
                }
                return poco;
            }
            catch (Exception e)
            {
                throw new QuestdbException(e);
            }
        }

        internal static QuestdbResponse AsQuestdbResponse(this string response)
        {
            return JsonConvert.DeserializeObject<QuestdbResponse>(response);
        }

        internal static Dictionary<PropertyInfo, int> MapCoulmsToProperties<T>(this List<TableColumn> columns)
        {
            Dictionary<PropertyInfo, int> propertyMap = new Dictionary<PropertyInfo, int>();

            var type = typeof(T);
            var properties = PropertyCache.GetOrAdd(type, _ => type.GetProperties());
            for (int i = 0; i < columns.Count; i++)
            {
                var col = columns[i];
                var property = properties.Where(w => w.Name.ToLower() == col.Name.ToLower()).FirstOrDefault();
                if (property != null)
                    propertyMap.Add(property, i);
            }
            return propertyMap;
        }

        internal static T As<T>(this string response)
        {
            return JsonConvert.DeserializeObject<T>(response);
        }

        private static void SetFieldValue<T>(T poco, PropertyInfo property, object value)
        {
            if (property == null || value == null || !property.CanWrite)
            {
                return;
            }

            try
            {
                var propertyType = property.PropertyType;

                //the same type
                if (propertyType == value.GetType())
                {
                    property.SetValue(poco, value);
                    return;
                }

                //handle time primitives
                if (propertyType == typeof(DateTime))
                {
                    property.SetValue(poco, ToDateTimeValue(value));
                    return;
                }

                if (propertyType == typeof(Instant))
                {
                    property.SetValue(poco, ToInstantValue(value));
                    return;
                }

                if (value is IConvertible)
                {
                    // Nullable types cannot be used in type conversion, but we can use Nullable.GetUnderlyingType()
                    // to determine whether the type is nullable and convert to the underlying type instead
                    var targetType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
                    property.SetValue(poco, Convert.ChangeType(value, targetType));
                }
                else
                {
                    property.SetValue(poco, value);
                }
            }
            catch (InvalidCastException ex)
            {
                throw new QuestdbException(
                    $"Class '{poco.GetType().Name}' field '{property.Name}' was defined with a different field type and caused an exception. " +
                    $"The correct type is '{value.GetType().Name}' (current field value: '{value}'). Exception: {ex.Message}", ex);
            }
        }

        private static DateTime ToDateTimeValue(object value)
        {
            if (value is DateTime dateTime)
            {
                return dateTime;
            }

            if (value is Instant instant)
            {
                return instant.InUtc().ToDateTimeUtc();
            }

            if (value is IConvertible)
            {
                return (DateTime)Convert.ChangeType(value, typeof(DateTime));
            }

            throw new InvalidCastException($"Object value of type {value.GetType().Name} cannot be converted to {nameof(DateTime)}");
        }

        private static Instant ToInstantValue(object value)
        {
            if (value is Instant instant)
            {
                return instant;
            }

            if (value is DateTime dateTime)
            {
                return Instant.FromDateTimeUtc(dateTime);
            }

            throw new InvalidCastException($"Object value of type {value.GetType().Name} cannot be converted to {nameof(Instant)}");
        }
    }
}