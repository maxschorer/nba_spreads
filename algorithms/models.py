import xgboost
from sklearn.svm import SVR
from sklearn.linear_model import LassoCV

from utils import *

K_FOLDS = 10
ALPHAS = [0.01, 0.1, 1, 10, 100]

def main():
  connection = get_connection()
  train_query = file_to_string('queries/train_data.sql')
  test_query = file_to_string('queries/test_data.sql')
  train_df = query_to_df(connection, train_query)
  y_train = train_df[train_df.columns[0]].values
  X_train = train_df[train_df.columns[1:]].values
  test_df = query_to_df(connection, test_query)
  y_test = test_df[test_df.columns[0]].values
  X_test = test_df[test_df.columns[2:]].values

  xgb = xgboost.XGBRegressor()
  svr = SVR()
  lasso = LassoCV(cv=K_FOLDS, alphas=ALPHAS)

  models = {'xgb': xgb, 'svr': svr, 'lasso': lasso}
  for model_name, model in models.iteritems():
    model.fit(X_train, y_train)
    pred = model.predict(X_test)
    test_df[model_name] = pred
    test_mse = pd.Series(pred.astype(float) - y_test.astype(float)).map(lambda x: x**2).mean()
    print '{} Test MSE: {}'.format(model_name, test_mse)
    pred = model.predict(X_train)
    train_mse = pd.Series(pred.astype(float) - y_train.astype(float)).map(lambda x: x**2).mean()
    print '{} Train MSE: {}'.format(model_name, train_mse)
