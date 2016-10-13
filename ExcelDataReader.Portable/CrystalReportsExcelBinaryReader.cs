﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ExcelDataReader.Portable.Core;
using ExcelDataReader.Portable.Core.BinaryFormat;
using ExcelDataReader.Portable.Data;
using ExcelDataReader.Portable.Log;

namespace ExcelDataReader.Portable
{
    /// <summary>
    /// A special reader for xls files generated by Crystal Reports
    /// It will read the whole document into memory and then builds up a table
    /// from the received cells
    /// </summary>
    public class CrystalReportsExcelBinaryReader : ExcelBinaryReader
    {
        public CrystalReportsExcelBinaryReader(IDataHelper dataHelper) : base(dataHelper)
        {

        }

        Stream m_file;
        XlsBiffStream m_excelStream;
        bool m_closed;
        string m_errorMessage;
        ushort m_version;
        bool IsV8 => m_version >= 0x600;
        List<XlsWorksheet> m_sheets;

        XlsHeader m_header;
        XlsWorkbookGlobals m_workbookGlobals;
        Encoding m_encoding = Encoding.Unicode;

        #region public overriden methods from base class

        public override Encoding Encoding => m_encoding;
        public override bool IsValid => string.IsNullOrWhiteSpace(m_errorMessage);
        public override string ExceptionMessage => m_errorMessage;
        public override string Name => m_sheets.FirstOrDefault()?.Name;
        public override string VisibleState => m_sheets.FirstOrDefault()?.VisibleState;
        public override int ResultsCount => m_workbookGlobals?.Sheets.Count ?? 0;
        public override ReadOption ReadOption
        {
            get
            {
                return ReadOption.Loose;
            }
            set
            {
                if (value != ReadOption.Loose) throw new NotSupportedException("For Reporting Services only ReadOption.Loose is supported");
            }
        }

        /// <summary>
        /// Opens the given stream and reads the workbook globals
        /// If an error occurs, the stream will be closed and an error will be written
        /// in the ExceptionMessage property 
        /// </summary>
        /// <param name="fileStream"></param>
        /// <returns></returns>
        public override async Task InitializeAsync(Stream fileStream)
        {
            m_file = fileStream;
            m_closed = false;

            await Task.Run(() => readWorkBookGlobals());
        }

        /// <summary>
        /// Loads all the data from the workbook into the given datasethelper
        /// </summary>
        /// <param name="datasetHelper">Dataset helper which gets filled with all the data</param>
        /// <returns></returns>
        public override async Task LoadDataSetAsync(IDatasetHelper datasetHelper)
        {
            await LoadDataSetAsync(datasetHelper, false);
        }

        /// <summary>
        /// Loads all the data from the workbook into the given datasethelper
        /// </summary>
        /// <param name="datasetHelper">Dataset helper which gets filled with all the data</param>
        /// <param name="convertOADateTime">Tries to convert all the readed datetime values</param>
        /// <returns></returns>
        public override async Task LoadDataSetAsync(IDatasetHelper datasetHelper, bool convertOADateTime)
        {
            datasetHelper.IsValid = IsValid;
            
            ConvertOaDate = convertOADateTime;
            datasetHelper.CreateNew();

            await Task.Run(() => readAllSheets(datasetHelper));

            Close();
            datasetHelper.DatasetLoadComplete();
        }

        public override bool Read()
        {
            throw new NotSupportedException("Read is not supported, only LoadDataSet");
        }

        /// <summary>
        /// Closes the open stream and releases all the resources
        /// </summary>
        public override void Close()
        {
            m_file?.Dispose();
            m_excelStream = null;
            m_sheets?.Clear();
            m_workbookGlobals = null;
            m_header = null;
            m_closed = true;
        }

        public override void Dispose()
        {
            Close();
            base.Dispose();
        }

        #endregion

        /// <summary>
        /// Reads all the global variables from the excel stream
        /// Logic copied from ExcelBinaryReader and cleaned up
        /// </summary>
        void readWorkBookGlobals()
        {
            try
            {
                m_header = XlsHeader.ReadHeader(m_file);
                var dir = new XlsRootDirectory(m_header);
                var workbookEntry = dir.FindEntry(WORKBOOK) ?? dir.FindEntry(BOOK);

                if (workbookEntry == null)
                {
                    throw new Exception(Errors.ErrorStreamWorkbookNotFound);
                }

                if (workbookEntry.EntryType != STGTY.STGTY_STREAM)
                {
                    throw new Exception(Errors.ErrorWorkbookIsNotStream);
                }

                m_excelStream = new XlsBiffStream(m_header, workbookEntry.StreamFirstSector, workbookEntry.IsEntryMiniStream, dir, this);

                m_workbookGlobals = new XlsWorkbookGlobals();

                m_excelStream.Seek(0, SeekOrigin.Begin);

                var rec = m_excelStream.Read();
                var bof = rec as XlsBiffBOF;

                if (bof == null || bof.Type != BIFFTYPE.WorkbookGlobals)
                {
                    throw new Exception(Errors.ErrorWorkbookGlobalsInvalidData);
                }

                var sst = false;

                m_version = bof.Version;
                m_sheets = new List<XlsWorksheet>();

                while (null != (rec = m_excelStream.Read()))
                {
                    switch (rec.ID)
                    {
                        case BIFFRECORDTYPE.INTERFACEHDR:
                            m_workbookGlobals.InterfaceHdr = (XlsBiffInterfaceHdr)rec;
                            break;
                        case BIFFRECORDTYPE.BOUNDSHEET:
                            var sheet = (XlsBiffBoundSheet)rec;

                            if (sheet.Type != XlsBiffBoundSheet.SheetType.Worksheet) break;

                            sheet.IsV8 = isV8();
                            this.Log().Debug("BOUNDSHEET IsV8={0}", sheet.IsV8);

                            m_sheets.Add(new XlsWorksheet(m_workbookGlobals.Sheets.Count, sheet));
                            m_workbookGlobals.Sheets.Add(sheet);

                            break;
                        case BIFFRECORDTYPE.MMS:
                            m_workbookGlobals.MMS = rec;
                            break;
                        case BIFFRECORDTYPE.COUNTRY:
                            m_workbookGlobals.Country = rec;
                            break;
                        case BIFFRECORDTYPE.CODEPAGE:

                            var encoding = ((XlsBiffSimpleValueRecord)rec).Value;
                            //set encoding based on code page name
                            //PCL does not supported codepage numbers
                            m_encoding = EncodingHelper.GetEncoding(encoding == 1200 ? (ushort)65001 : encoding);
                            break;
                        case BIFFRECORDTYPE.FONT:
                        case BIFFRECORDTYPE.FONT_V34:
                            m_workbookGlobals.Fonts.Add(rec);
                            break;
                        case BIFFRECORDTYPE.FORMAT_V23:
                            {
                                var fmt = (XlsBiffFormatString)rec;
                                m_workbookGlobals.Formats.Add((ushort)m_workbookGlobals.Formats.Count, fmt);
                            }
                            break;
                        case BIFFRECORDTYPE.FORMAT:
                            {
                                var fmt = (XlsBiffFormatString)rec;
                                m_workbookGlobals.Formats.Add(fmt.Index, fmt);
                            }
                            break;
                        case BIFFRECORDTYPE.XF:
                        case BIFFRECORDTYPE.XF_V4:
                        case BIFFRECORDTYPE.XF_V3:
                        case BIFFRECORDTYPE.XF_V2:
                            m_workbookGlobals.ExtendedFormats.Add(rec);
                            break;
                        case BIFFRECORDTYPE.SST:
                            m_workbookGlobals.SST = (XlsBiffSST)rec;
                            sst = true;
                            break;
                        case BIFFRECORDTYPE.CONTINUE:
                            if (!sst) break;
                            var contSst = (XlsBiffContinue)rec;
                            m_workbookGlobals.SST.Append(contSst);
                            break;
                        case BIFFRECORDTYPE.EXTSST:
                            m_workbookGlobals.ExtSST = rec;
                            sst = false;
                            break;
                        case BIFFRECORDTYPE.PROTECT:
                        case BIFFRECORDTYPE.PASSWORD:
                        case BIFFRECORDTYPE.PROT4REVPASSWORD:
                            break;
                        case BIFFRECORDTYPE.EOF:
                            m_workbookGlobals.SST?.ReadStrings();
                            return;

                        default:
                            continue;
                    }
                }
            }
            catch (Exception ex)
            {
                setError(ex.Message);
            }
        }

        /// <summary>
        /// Reads all the sheets and writes the data into the datasetHelper
        /// </summary>
        /// <param name="datasetHelper">datasetHelper which get's filled with all the data</param>
        void readAllSheets(IDatasetHelper datasetHelper)
        {
            if (m_closed) return;
            foreach (var sheet in m_sheets)
            {
                readSheet(sheet, datasetHelper);
            }
        }

        /// <summary>
        /// Reads the data of a single sheet into the datasethelper
        /// </summary>
        /// <param name="sheet">Excel sheet to read</param>
        /// <param name="datasetHelper">datasetHelper which get's filled with all the data</param>
        void readSheet(XlsWorksheet sheet, IDatasetHelper datasetHelper)
        {
            SheetGlobals header;
            try
            {
                header = readWorkSheetGlobals(sheet);
            }
            catch (Exception ex)
            {
                this.Log().Warn($"Failed to read globals for sheet {sheet.Name} ({sheet.Index}): {ex.Message}");
                datasetHelper.IsValid = false;
                return;
            }

            //Read all the content from the sheet
            var activeSheetCells = readWorkSheetData(header);
            //And write the data into the datasethelper
            datasetHelper.CreateNewTable(sheet.Name);
            datasetHelper.AddExtendedPropertyToTable("visiblestate", sheet.VisibleState);
            datasetHelper.BeginLoadData();
            writeColumns(datasetHelper, activeSheetCells, header);
            writeDataToDataSet(activeSheetCells, datasetHelper);
            datasetHelper.EndLoadTable();
        }

        /// <summary>
        /// Reads all the cell data from the excel stream into a dictionary
        /// </summary>
        /// <param name="header">Sheet header data</param>
        /// <returns>Dictionary with all the data: 
        /// Key = cell reference eg 1:5 or 5:8 used for fast lookup
        /// Value = ExcelCell with data
        /// </returns>
        Dictionary<string, ExcelCell> readWorkSheetData(SheetGlobals header)
        {
            var sheetData = new Dictionary<string, ExcelCell>();

            foreach (var index in header.Index.DbCellAddresses)
            {
                var rowOffset = findFirstDataCellOffset((int) index);
                if(rowOffset == -1) continue;

                readWorkSheetDataFromOffset(rowOffset, header, sheetData);
            }

            return sheetData;
        }

        /// <summary>
        /// Reads all the data starting at a specific cell Offset
        /// All the cells will be added to the sheetData variable
        /// </summary>
        /// <param name="cellOffset">cellOffset to start reading from</param>
        /// <param name="sheetGlobals">Global information about the sheet</param>
        /// <param name="sheetData">Dictionary with all the cells for this sheet</param>
        void readWorkSheetDataFromOffset(int cellOffset, SheetGlobals sheetGlobals, Dictionary<string, ExcelCell> sheetData)
        {
            while (cellOffset < m_excelStream.Size)
            {
                var rec = m_excelStream.ReadAt(cellOffset);
                cellOffset += rec.Size;

                if ((rec is XlsBiffDbCell) || (rec is XlsBiffMSODrawing)) { continue; }
                if (rec is XlsBiffEOF) { return; }

                var cell = rec as XlsBiffBlankCell;
                if ((null == cell) || (cell.ColumnIndex >= sheetGlobals.Columns)) continue;

                addCell(sheetData, cell, sheetGlobals);
            }
        }

        /// <summary>
        /// Seeks for an offset of the first data cell starting 
        /// from the given offset
        /// </summary>
        /// <param name="startOffset">Start offset</param>
        /// <returns>Offset of the first datacell after the start offset
        /// Or -1 if no data cell is found</returns>
        int findFirstDataCellOffset(int startOffset)
        {
            //seek to the first dbcell record
            var record = m_excelStream.ReadAt(startOffset);
            while (!(record is XlsBiffDbCell))
            {
                if (m_excelStream.Position >= m_excelStream.Size) return -1;
                if (record is XlsBiffEOF) return -1;

                record = m_excelStream.Read();
            }

            var startCell = (XlsBiffDbCell)record;
            var offs = startCell.RowAddress;

            do
            {
                var row = m_excelStream.ReadAt(offs) as XlsBiffRow;
                if (row == null) break;
                offs += row.Size;
            } while (true);

            return offs;
        }

        /// <summary>
        /// Adds/overwrites the given cell to the values dictionary
        ///     If cell is a multi record all the values of the multi record cell get readed
        /// </summary>
        /// <param name="values">Dictionary with all the cells</param>
        /// <param name="cell">Current excel cell which value should be read and added to the values dictionary</param>
        /// <param name="sheetGlobals">Sheet information</param>
        void addCell(IDictionary<string, ExcelCell> values, XlsBiffBlankCell cell, SheetGlobals sheetGlobals)
        {
            if (cell.ID == BIFFRECORDTYPE.MULRK)
            {
                var multiRecordsCell = (XlsBiffMulRKCell) cell;
                for (var col = cell.ColumnIndex; col <= multiRecordsCell.LastColumnIndex; col++)
                {
                    var newValue = multiRecordsCell.GetValue(col);
                    var mergedCellValue = convertOaDateTime(newValue, multiRecordsCell.GetXF(col));
                    addCell(values, mergedCellValue, col, cell.RowIndex, sheetGlobals);
                }
            }
            else
            {
                var value = readCellValue(cell);
                addCell(values, value, cell.ColumnIndex, cell.RowIndex, sheetGlobals);
            }
        }

        /// <summary>
        /// Adds/overwrites the value to the dictionary
        /// Only if the column and row index is valid based on the sheet information
        /// </summary>
        /// <param name="cells">Dictionary with all the cells</param>
        /// <param name="value">Cell value which needs to be added</param>
        /// <param name="columnIndex">Cell column index which needs to be added</param>
        /// <param name="rowIndex">Cell row index which needs to be added</param>
        /// <param name="sheetGlobals">Sheet information</param>
        void addCell(IDictionary<string, ExcelCell> cells, object value, ushort columnIndex, ushort rowIndex, SheetGlobals sheetGlobals)
        {
            if (rowIndex >= sheetGlobals.Rows || columnIndex >= sheetGlobals.Columns)
            {
                LogManager.Log(this).Warn("Cannot write value in cell({0},{1}): {2}", rowIndex, columnIndex, value);
                return;
            }
            
            var key = $"{rowIndex}:{columnIndex}";
            if (!cells.ContainsKey(key))
            {
                cells.Add(key, new ExcelCell(value, columnIndex, rowIndex));
                return;
            }
            var existingCell = cells[key];
            var existingValue = existingCell.Value?.ToString();
            var newValue = value?.ToString();
            if (existingValue == newValue) return;
            this.Log().Info($"Overwrite value '{existingValue}' with '{newValue}' on {rowIndex}:{columnIndex}");
            existingCell.Value = value;
        }

        /// <summary>
        /// Reads the value from the excel cell 
        /// and tries to convert it into a strong typed value
        /// </summary>
        /// <param name="cell">Excel cell</param>
        /// <returns>Strongly typed value</returns>
        object readCellValue(XlsBiffBlankCell cell)
        {
            double dValue;
            switch (cell.ID)
            {
                case BIFFRECORDTYPE.BOOLERR:
                    if (cell.ReadByte(7) == 0)
                        return cell.ReadByte(6) != 0;
                    break;
                case BIFFRECORDTYPE.BOOLERR_OLD:
                    if (cell.ReadByte(8) == 0)
                        return cell.ReadByte(7) != 0;
                    break;
                case BIFFRECORDTYPE.INTEGER:
                case BIFFRECORDTYPE.INTEGER_OLD:
                    return ((XlsBiffIntegerCell)cell).Value;
                case BIFFRECORDTYPE.NUMBER:
                case BIFFRECORDTYPE.NUMBER_OLD:
                    dValue = ((XlsBiffNumberCell)cell).Value;
                    return convertOaDateTime(dValue, cell.XFormat);
                case BIFFRECORDTYPE.LABEL:
                case BIFFRECORDTYPE.LABEL_OLD:
                case BIFFRECORDTYPE.RSTRING:
                    return ((XlsBiffLabelCell)cell).Value;
                case BIFFRECORDTYPE.LABELSST:
                    return m_workbookGlobals.SST.GetString(((XlsBiffLabelSSTCell)cell).SSTIndex);
                case BIFFRECORDTYPE.RK:
                    dValue = ((XlsBiffRKCell)cell).Value;
                    return convertOaDateTime(dValue, cell.XFormat);
                case BIFFRECORDTYPE.MULRK:
                    
                case BIFFRECORDTYPE.BLANK:
                case BIFFRECORDTYPE.BLANK_OLD:
                case BIFFRECORDTYPE.MULBLANK:
                    // Skip blank cells
                    break;
                case BIFFRECORDTYPE.FORMULA:
                case BIFFRECORDTYPE.FORMULA_OLD:
                    var oValue = ((XlsBiffFormulaCell)cell).Value;
                    if (oValue is FORMULAERROR)
                    {
                        oValue = null;
                    }
                    else
                    {
                        oValue = convertOaDateTime(oValue, (cell.XFormat));//date time offset
                    }
                    return oValue;
            }

            return null;
        }

        /// <summary>
        /// Only if ConvertOaDate is true 
        /// it will try to convert the double with the given excel  format into a date time
        /// If it fails, it will return the given value
        /// </summary>
        /// <param name="value">Value to convert</param>
        /// <param name="xFormat">DateTime format</param>
        /// <returns>DateTime on success, value on error</returns>
        object convertOaDateTime(double value, ushort xFormat)
        {
            return ConvertOaDate ? 
                tryConvertOaDateTime(value, xFormat) : 
                value;
        }

        /// <summary>
        /// Only if ConvertOaDate is true 
        /// it will try to convert the value with the given excel  format into a date time
        /// If it fails, it will return the given value
        /// </summary>
        /// <param name="value">Value to convert</param>
        /// <param name="xFormat">DateTime format</param>
        /// <returns>DateTime on success, value on error</returns>
        object convertOaDateTime(object value, ushort xFormat)
        {
            if (!ConvertOaDate) return value;
            if (value == null) return null;

            double dValue;
            return double.TryParse(value.ToString(), out dValue) ? 
                tryConvertOaDateTime(dValue, xFormat) : 
                value;
        }

        /// <summary>
        /// Convert the value with the given excel format into a date time
        /// If it fails, it will return the given value
        /// </summary>
        /// <param name="value">Value to convert</param>
        /// <param name="xFormat">DateTime format</param>
        /// <returns>DateTime on success, value on error</returns>
        object tryConvertOaDateTime(double value, ushort xFormat)
        {
            ushort format;
            if (xFormat < m_workbookGlobals.ExtendedFormats.Count)
            {
                var rec = m_workbookGlobals.ExtendedFormats[xFormat];
                switch (rec.ID)
                {
                    case BIFFRECORDTYPE.XF_V2:
                        format = (ushort)(rec.ReadByte(2) & 0x3F);
                        break;
                    case BIFFRECORDTYPE.XF_V3:
                        if ((rec.ReadByte(3) & 4) == 0)
                            return value;
                        format = rec.ReadByte(1);
                        break;
                    case BIFFRECORDTYPE.XF_V4:
                        if ((rec.ReadByte(5) & 4) == 0)
                            return value;
                        format = rec.ReadByte(1);
                        break;
                    default:
                        if ((rec.ReadByte(m_workbookGlobals.Sheets[m_workbookGlobals.Sheets.Count - 1].IsV8 ? 9 : 7) & 4) == 0)
                            return value;
                        format = rec.ReadUInt16(2);
                        break;
                }
            }
            else
            {
                format = xFormat;
            }

            switch (format)
            {
                // numeric built in formats
                case 0: //"General";
                case 1: //"0";
                case 2: //"0.00";
                case 3: //"#,##0";
                case 4: //"#,##0.00";
                case 5: //"\"$\"#,##0_);(\"$\"#,##0)";
                case 6: //"\"$\"#,##0_);[Red](\"$\"#,##0)";
                case 7: //"\"$\"#,##0.00_);(\"$\"#,##0.00)";
                case 8: //"\"$\"#,##0.00_);[Red](\"$\"#,##0.00)";
                case 9: //"0%";
                case 10: //"0.00%";
                case 11: //"0.00E+00";
                case 12: //"# ?/?";
                case 13: //"# ??/??";
                case 0x30:// "##0.0E+0";

                case 0x25:// "_(#,##0_);(#,##0)";
                case 0x26:// "_(#,##0_);[Red](#,##0)";
                case 0x27:// "_(#,##0.00_);(#,##0.00)";
                case 40:// "_(#,##0.00_);[Red](#,##0.00)";
                case 0x29:// "_(\"$\"* #,##0_);_(\"$\"* (#,##0);_(\"$\"* \"-\"_);_(@_)";
                case 0x2a:// "_(\"$\"* #,##0_);_(\"$\"* (#,##0);_(\"$\"* \"-\"_);_(@_)";
                case 0x2b:// "_(\"$\"* #,##0.00_);_(\"$\"* (#,##0.00);_(\"$\"* \"-\"??_);_(@_)";
                case 0x2c:// "_(* #,##0.00_);_(* (#,##0.00);_(* \"-\"??_);_(@_)";
                    return value;

                // date formats
                case 14: //this.GetDefaultDateFormat();
                case 15: //"D-MM-YY";
                case 0x10: // "D-MMM";
                case 0x11: // "MMM-YY";
                case 0x12: // "h:mm AM/PM";
                case 0x13: // "h:mm:ss AM/PM";
                case 20: // "h:mm";
                case 0x15: // "h:mm:ss";
                case 0x16: // string.Format("{0} {1}", this.GetDefaultDateFormat(), this.GetDefaultTimeFormat());

                case 0x2d: // "mm:ss";
                case 0x2e: // "[h]:mm:ss";
                case 0x2f: // "mm:ss.0";
                    return Helpers.ConvertFromOATime(value);
                case 0x31:// "@";
                    return value.ToString();

                default:
                    XlsBiffFormatString fmtString;
                    if (m_workbookGlobals.Formats.TryGetValue(format, out fmtString))
                    {
                        var fmt = fmtString.Value;
                        var formatReader = new FormatReader() { FormatString = fmt };
                        if (formatReader.IsDateFormatString())
                            return Helpers.ConvertFromOATime(value);
                    }
                    return value;
            }
        }

        /// <summary>
        /// Reads all the sheet variables from the excel stream
        /// Logic copied from ExcelBinaryReader and cleaned up
        /// </summary>
        /// <returns>A SheetGlobals object with the properties set</returns>
        SheetGlobals readWorkSheetGlobals(XlsWorksheet sheet)
        {
            var data = new SheetGlobals();

            m_excelStream.Seek((int)sheet.DataOffset, SeekOrigin.Begin);

            var bof = m_excelStream.Read() as XlsBiffBOF;
            if (bof == null || bof.Type != BIFFTYPE.Worksheet) throw new Exception("Failed to read XLS BOF");

            var rec = m_excelStream.Read();
            if (rec == null) throw new Exception("Failed to read first record");
            if (rec is XlsBiffIndex)
            {
                data.Index = rec as XlsBiffIndex;
            }
            else if (rec is XlsBiffUncalced)
            {
                // Sometimes this come before the index...
                data.Index = m_excelStream.Read() as XlsBiffIndex;
            }


            if (data.Index == null)
            {
                throw new Exception("Failed to read the Index record, which is required");
            }

            data.Index.IsV8 = IsV8;
            this.Log().Debug("INDEX IsV8={0}", data.Index.IsV8);

            XlsBiffRecord trec;
            XlsBiffDimensions dims = null;

            do
            {
                trec = m_excelStream.Read();
                if (trec.ID != BIFFRECORDTYPE.DIMENSIONS) continue;

                dims = (XlsBiffDimensions)trec;
                break;
            } while (trec.ID != BIFFRECORDTYPE.ROW);

            XlsBiffRow rowRecord = null;
            while (rowRecord == null)
            {
                if (m_excelStream.Position >= m_excelStream.Size)
                    break;
                var thisRec = m_excelStream.Read();

                this.Log().Debug("finding rowRecord offset {0}, rec: {1}", thisRec.Offset, thisRec.ID);
                if (thisRec is XlsBiffEOF)
                    break;
                rowRecord = thisRec as XlsBiffRow;
            }

            if (rowRecord != null) this.Log().Debug("Got row {0}, rec: id={1},rowindex={2}, rowColumnStart={3}, rowColumnEnd={4}", rowRecord.Offset, rowRecord.ID, rowRecord.RowIndex, rowRecord.FirstDefinedColumn, rowRecord.LastDefinedColumn);

            data.Row = rowRecord;

            if (dims != null)
            {
                dims.IsV8 = isV8();
                this.Log().Debug("dims IsV8={0}", dims.IsV8);
                data.Columns = dims.LastColumn - 1;

                //handle case where sheet reports last column is 1 but there are actually more
                if (data.Columns <= 0 && rowRecord != null)
                {
                    data.Columns = rowRecord.LastDefinedColumn;
                }

                data.Rows = (int)dims.LastRow;
                sheet.Dimensions = dims;
            }
            else
            {
                data.Columns = 256;
                data.Rows = (int)data.Index.LastExistingRow;
            }

            if (data.Index.LastExistingRow <= data.Index.FirstExistingRow)
            {
                throw new Exception($"Last row ({data.Index.LastExistingRow}) <= first row ({data.Index.FirstExistingRow})");
            }

            if (data.Row == null)
            {
                throw new Exception("Failed to read the first data record");
            }

            return data;
        }

        /// <summary>
        /// Prepares the datasethelper by adding the columns for this sheet
        /// If IsFirstRowAsColumnNames it will use the first row from the sheet as column names
        /// If IsFirstRowAsColumnNames the first row will also be deleted fromt the sheetdata dictionary
        /// Else it will just add columns without a name
        /// </summary>
        /// <param name="datasetHelper">Datasethelper add the columns to</param>
        /// <param name="sheetData">Excel cells for this sheet</param>
        /// <param name="header">Sheet global properties</param>
        void writeColumns(IDatasetHelper datasetHelper, Dictionary<string, ExcelCell> sheetData, SheetGlobals header)
        {
            var firstRow = sheetData.Where(c => c.Value.Row == 0).ToList();
            for (var index = 0; index < header.Columns; index++)
            {
                string name = null; //by default use no column names
                if (IsFirstRowAsColumnNames)
                {
                    name = firstRow.FirstOrDefault(c => c.Value.Column == index) //Search for matching excel cell
                        .Value? //If it is found and not null
                        .Value? //Read the value from the cell
                        .ToString(); //And convert it into a string
                    if (string.IsNullOrWhiteSpace(name)) //If this name is null
                    {
                        name = string.Concat(COLUMN, index); //then use default names like: Column1, Column2     
                    }
                }
                datasetHelper.AddColumn(name);
            }
            if (IsFirstRowAsColumnNames)
            {
                //Clean up the cells from the first row, because we used them as columns
                foreach (var col in firstRow)
                {
                    sheetData.Remove(col.Key);
                }
            }
        }

        /// <summary>
        /// Writes all the cell values to the datasethelper
        /// </summary>
        /// <param name="cells">All cell values</param>
        /// <param name="datasetHelper">Datasethelper to fill with the cell values</param>
        void writeDataToDataSet(Dictionary<string, ExcelCell> cells, IDatasetHelper datasetHelper)
        {
            var sheetCells = cells.Values.GroupBy(c => c.Row).OrderBy(c => c.Key);
            foreach (var row in sheetCells)
            {
                var columns = row.Max(c => c.Column);
                var data = new object[columns + 1];
                for (var col = 0; col <= columns; col++)
                {
                    data[col] = row.FirstOrDefault(c => c.Column == col)?.Value;
                }
                datasetHelper.AddRow(data);
            }
        }

        /// <summary>
        /// It will set the error message 
        /// And then closes the stream
        /// </summary>
        /// <param name="message"></param>
        void setError(string message)
        {
            m_errorMessage = message;
            Close();
        }
    }

    class SheetGlobals
    {
        public XlsBiffIndex Index { get; set; }
        public XlsBiffRow Row { get; set; }
        public int Columns { get; set; }
        public int Rows { get; set; }
    }

    class ExcelCell
    {
        public int Column { get; private set; }
        public int Row { get; private set; }
        public object Value { get; set; }

        public ExcelCell(object value, int column, int row)
        {
            Value = value;
            Column = column;
            Row = row;
        }
    }
}