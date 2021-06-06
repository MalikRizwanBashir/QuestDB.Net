using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using NodaTime;

namespace Questdb.Net.Write
{
    internal class PropertyInfoColumn
    {
        internal PropertyInfo Property;
        internal Column Column;
    }

    internal class Mapper
    {
        private static readonly ConcurrentDictionary<string, PropertyInfoColumn[]> CACHE = new ConcurrentDictionary<string, PropertyInfoColumn[]>();
        internal PointData ToPoint<TM>(TM measurement, WritePrecision precision)
        {
            Arguments.CheckNotNull(measurement, nameof(measurement));
            Arguments.CheckNotNull(precision, nameof(precision));

            var measurementType = measurement.GetType();
            CacheMeasurementClass(measurementType);
            var measurementName = string.Empty;
            var measurementAttribute = (Table)measurementType.GetCustomAttribute(typeof(Table));
            if (measurementAttribute == null)
            {
                measurementName = typeof(TM).Name;
                //throw new InvalidOperationException(
                //    $"Measurement {measurement} does not have a {typeof(Measurement)} attribute.");
            }
            else
            {
                measurementName = measurementAttribute.Name;
            }

            var point = PointData.Measurement(measurementName);

            foreach (var propertyInfo in CACHE[measurementType.Name])
            {
                var value = propertyInfo.Property.GetValue(measurement);
                if (value == null)
                {
                    continue;
                }

                var name = !string.IsNullOrEmpty(propertyInfo.Column?.Name) ? propertyInfo.Column.Name : propertyInfo.Property.Name;
                if (propertyInfo.Column != null && propertyInfo.Column.IsTag)
                {
                    point = point.Tag(name, value.ToString());
                }
                else if (propertyInfo.Column != null && propertyInfo.Column.IsTimestamp)
                {
                    if (value is long l)
                    {
                        point = point.Timestamp(l, precision);
                    }
                    else if (value is TimeSpan span)
                    {
                        point = point.Timestamp(span, precision);
                    }
                    else if (value is DateTime date)
                    {
                        point = point.Timestamp(date, precision);
                    }
                    else if (value is DateTimeOffset offset)
                    {
                        point = point.Timestamp(offset, precision);
                    }
                    else if (value is Instant instant)
                    {
                        point = point.Timestamp(instant, precision);
                    }
                    else
                    {
                        Trace.WriteLine($"{value} is not supported as Timestamp");
                    }
                }
                else
                {
                    if (value is bool b)
                    {
                        point = point.Field(name, b);
                    }
                    else if (value is double d)
                    {
                        point = point.Field(name, d);
                    }
                    else if (value is float f)
                    {
                        point = point.Field(name, f);
                    }
                    else if (value is decimal dec)
                    {
                        point = point.Field(name, dec);
                    }
                    else if (value is long lng)
                    {
                        point = point.Field(name, lng);
                    }
                    else if (value is ulong ulng)
                    {
                        point = point.Field(name, ulng);
                    }
                    else if (value is int i)
                    {
                        point = point.Field(name, i);
                    }
                    else if (value is byte bt)
                    {
                        point = point.Field(name, bt);
                    }
                    else if (value is sbyte sb)
                    {
                        point = point.Field(name, sb);
                    }
                    else if (value is short sh)
                    {
                        point = point.Field(name, sh);
                    }
                    else if (value is uint ui)
                    {
                        point = point.Field(name, ui);
                    }
                    else if (value is ushort us)
                    {
                        point = point.Field(name, us);
                    }
                    else
                    {
                        point = point.Field(name, value.ToString());
                    }
                }
            }

            return point;
        }

        private void CacheMeasurementClass(Type measurementType)
        {
            if (CACHE.ContainsKey(measurementType.Name))
            {
                return;
            }

            CACHE[measurementType.Name] = measurementType.GetProperties()
                .Select(property => new PropertyInfoColumn { Column = (Column)property.GetCustomAttribute(typeof(Column)), Property = property })
                .ToArray();
        }
    }
}