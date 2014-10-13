using System.Collections.Generic;
using Legacy1C77.Import.Service.DAL;

namespace Legacy1C77.Import.Service.Core
{
    public abstract class BaseImportFrom1Cv77
    {
        protected comClient1Cv77Adatpor _comClient1Cv77Adatpor = null;
        protected sqlAdaptor _sqlAdaptor = null;

        #region Properties
        private IList<TableColumn> _sqlColumns = null;
        protected IList<TableColumn> SqlColumns
        {
            get
            {
                if (_sqlColumns == null)
                {
                    _sqlColumns = _sqlAdaptor.getTableColumns();
                }
                return _sqlColumns;
            }
        }

        private IList<TableColumn> _1cv77CatalogColumns = null;
        protected IList<TableColumn> OneCv77CatalogColumns
        {
            get
            {
                if (_1cv77CatalogColumns == null)
                {
                    _1cv77CatalogColumns = _comClient1Cv77Adatpor.getCatalogTableColumns();
                }
                return _1cv77CatalogColumns;
            }
        }

        private IList<TableColumn> _1cv77DocumentsColumns = null;
        protected IList<TableColumn> OneCv77DocumentsColumns
        {
            get
            {
                if (_1cv77DocumentsColumns == null)
                {
                    _1cv77DocumentsColumns = _comClient1Cv77Adatpor.getDocumentTableColumns();
                }
                return _1cv77DocumentsColumns;
            }
        }

        private IList<TableColumn> _1cv77DocumentsHeaderColumns = null;
        protected IList<TableColumn> OneCv77DocumentsHeaderColumns
        {
            get
            {
                if (_1cv77DocumentsHeaderColumns == null)
                {
                    _1cv77DocumentsHeaderColumns = _comClient1Cv77Adatpor.getDocumentHeaderColumns();
                }
                return _1cv77DocumentsHeaderColumns;
            }
        }

        
        #endregion

        protected BaseImportFrom1Cv77(comClient1Cv77Adatpor com1Cv77Adaptor, sqlAdaptor sqlAdaptor)
        {
            this._comClient1Cv77Adatpor = com1Cv77Adaptor;
            this._sqlAdaptor = sqlAdaptor;
        }

        public abstract bool Sync();

    }
}
