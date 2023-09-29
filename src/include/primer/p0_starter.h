//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  Matrix(int rows, int cols) : rows_(rows), cols_(cols) { linear_ = new T[rows * cols]; }

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;

  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix */
  virtual int GetRowCount() const = 0;

  /** @return The number of columns in the matrix */
  virtual int GetColumnCount() const = 0;

  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual T GetElement(int i, int j) const = 0;

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) = 0;

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) = 0;

  /**
   * Destroy a matrix instance.
   * TODO(P0): Add implementation
   */
  virtual ~Matrix() { delete[] linear_; }
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    data_ = new T *[rows];
    for (int i = 0; i < rows; i++) {
      data_[i] = new T[cols];
    }
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix
   */
  int GetRowCount() const override { return this->rows_; }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  int GetColumnCount() const override { return this->cols_; }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  T GetElement(int i, int j) const override {
    if (i < 0 || i >= this->rows_ || j < 0 || j >= this->cols_) {
      throw Exception{ExceptionType::OUT_OF_RANGE, "Out of range access."};
    }

    return data_[i][j];
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {
    if (i < 0 || i >= this->rows_ || j < 0 || j >= this->cols_) {
      throw Exception{ExceptionType::OUT_OF_RANGE, "Out of range access."};
    }

    this->linear_[i * this->cols_ + j] = val;
    data_[i][j] = val;
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  void FillFrom(const std::vector<T> &source) override {
    int size = source.size();
    if (this->rows_ * this->cols_ != size) {
      throw Exception{ExceptionType::OUT_OF_RANGE, "Matrics size is different compared to source."};
    }

    for (auto i = 0; i < size; i++) {
      this->linear_[i] = source[i];
      data_[i / this->cols_][i % this->cols_] = source[i];
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  ~RowMatrix() override {
    if (data_ != nullptr) {
      for (int i = 0; i < this->rows_; i++) {
        if (data_[i] != nullptr) {
          delete[] data_[i];
        }
      }

      delete[] data_;
    }
  }

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear` array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    int row_a_count = matrixA->GetRowCount();
    int col_a_count = matrixA->GetRowCount();

    if (!AreMatrixsRowAndColMatchForAdd(matrixA, matrixB)) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    std::unique_ptr<RowMatrix<T>> res = std::make_unique<RowMatrix<T>>(row_a_count, col_a_count);
    for (int i = 0; i < row_a_count; i++) {
      for (int j = 0; j < col_a_count; j++) {
        res->SetElement(i, j, matrixA->GetElement(i, j) + matrixB->GetElement(i, j));
      }
    }

    return res;
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */
  static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    int row_a_count = matrixA->GetRowCount();
    int col_a_count = matrixA->GetColumnCount();
    int col_b_count = matrixB->GetColumnCount();
    if (!AreMatrixsRowAndColMatchForMultiply(matrixA, matrixB)) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    std::unique_ptr<RowMatrix<T>> res = std::make_unique<RowMatrix<T>>(row_a_count, col_b_count);
    for (int i = 0; i < row_a_count; i++) {
      for (int j = 0; j < col_b_count; j++) {
        T sum = matrixA->GetElement(i, j);
        bool is_sum_initialized = false;

        for (int p = 0; p < col_a_count; p++) {
          if (!is_sum_initialized) {
            sum = matrixA->GetElement(i, p) * matrixB->GetElement(p, j);
            is_sum_initialized = true;
          } else {
            sum += matrixA->GetElement(i, p) * matrixB->GetElement(p, j);
          }
        }

        res->SetElement(i, j, sum);
      }
    }

    return res;
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB,
                                            const RowMatrix<T> *matrixC) {
    std::unique_ptr<RowMatrix<T>> res1 = Multiple(matrixA, matrixB);
    if (res1 == nullptr) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    return Add(res1.get(), matrixC);
  }

  static bool AreMatrixsRowAndColMatchForAdd(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    int row_a_count = matrixA->GetRowCount();
    int row_b_count = matrixB->GetRowCount();
    int col_a_count = matrixA->GetColumnCount();
    int col_b_count = matrixB->GetColumnCount();

    return (row_a_count == row_b_count) && (col_a_count == col_b_count);
  }

  static bool AreMatrixsRowAndColMatchForMultiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    int row_b_count = matrixB->GetRowCount();
    int col_a_count = matrixA->GetColumnCount();

    return (col_a_count == row_b_count);
  }
};
}  // namespace bustub
