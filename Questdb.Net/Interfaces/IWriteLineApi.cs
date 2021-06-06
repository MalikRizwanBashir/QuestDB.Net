using Questdb.Net.Write;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Questdb.Net
{
    public interface IWriteLineApi
    {
        #region write points, line protocol
        /// <summary>
        /// Write a Data point into specified database.
        /// </summary>
        /// <param name="point">specifies the Data point to write into database</param>
        void WritePoint(PointData point);

        /// <summary>
        /// Write Data points into specified database.
        /// </summary>
        /// <param name="points">specifies the Data points to write into database</param>
        void WritePoints(List<PointData> points);

        /// <summary>
        /// Write Data points into specified database.
        /// </summary>
        /// <param name="points">specifies the Data points to write into database</param>
        void WritePoints(params PointData[] points);
        #endregion

        #region write T type as point, Line protocol
        /// <summary>
        /// Write a Measurement into specified database.
        /// </summary>
        /// <param name="measurement">specifies the Measurement to write into database</param>
        /// <typeparam name="TM">measurement type</typeparam>
        void WriteMeasurement<TM>(TM measurement);

        /// <summary>
        /// Write Measurements into specified database.
        /// </summary>
        /// <param name="measurements">specifies Measurements to write into database</param>
        /// <typeparam name="TM">measurement type</typeparam>
        void WriteMeasurements<TM>(List<TM> measurements);

        /// <summary>
        /// Write Measurements into specified database.
        /// </summary>
        /// <param name="measurements">specifies Measurements to write into database</param>
        /// <typeparam name="TM">measurement type</typeparam>
        void WriteMeasurements<TM>(params TM[] measurements);
        #endregion
    }
}
