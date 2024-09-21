// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/bcfl"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"math/big"
	"runtime"
	"sync"
	"time"
)

// StateProcessor is a basic Processor, which takes care of transitioning
// state from one point to another.
//
// StateProcessor implements Processor.
type StateProcessor struct {
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain
	engine consensus.Engine    // Consensus engine used for block rewards
}

// NewStateProcessor initialises a new StateProcessor.
func NewStateProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *StateProcessor {
	return &StateProcessor{
		config: config,
		bc:     bc,
		engine: engine,
	}
}

// Process processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb and applying any rewards to both
// the processor (coinbase) and any included uncles.
//
// Process returns the receipts and logs accumulated during the process and
// returns the amount of gas that was used in the process. If any of the
// transactions failed to execute due to insufficient gas it will return an error.
func (p *StateProcessor) Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (types.Receipts, []*types.Log, uint64, error) {

	var (
		receipts    types.Receipts
		usedGas     = new(uint64)
		header      = block.Header()
		blockHash   = block.Hash()
		blockNumber = block.Number()
		allLogs     []*types.Log
		gp          = new(GasPool).AddGas(block.GasLimit())
	)
	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	blockContext := NewEVMBlockContext(header, p.bc, nil)
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg) // *EVM

	// Iterate over and process the individual transactions
	//if block.Transactions().Len() != 0 {
	//	fmt.Printf("\n[Lin-Process]: --------------------------------------------------------- \n")
	//	for index, v := range block.Transactions() {
	//		fmt.Printf("当前交易是: %v, Parent: %v, Child: %v \n", index, *(v.Parent()), *(v.Child()))
	//	}
	//	fmt.Printf("[Lin-Process]: --------------------------------------------------------- \n\n")
	//}

	// 判断是否是并行交易
	isBSSTORE, err := bcfl.HexToByteArray(bcfl.SetUpdates)
	if err != nil {
		fmt.Printf("[Lin] Error HexToByteArray: %v \n", err)
	}
	isBAGG, err := bcfl.HexToByteArray(bcfl.Aggregate)
	if err != nil {
		fmt.Printf("[Lin] Error HexToByteArray: %v \n", err)
	}

	var (
		cocrTxs       types.Transactions // 按block内的顺序,保存需要并发执行的交易 *Transaction
		cocrMsgs      []*types.Message   // 按block内的顺序,保存需要并发执行交易的msg结构
		cocrIndex     []int              // 按block内的顺序,保存需要并发执行的交易的索引
		cocrWaitGroup sync.WaitGroup     // 并发等待组
	)

	var msgToReceipt sync.Map                  // *msg -> *receipt 的映射
	indexToMsg := make(map[int]*types.Message) // index -> *msg 的映射

	var serialTimeOverhead time.Duration
	serialTxs := 0

	for i, tx := range block.Transactions() {
		// 如果是需要并发的交易,整理成一个需要并发的tx切片传递给并发程序，并且wg+1,wg要指针传递，数组要指针传递，vmenv要数值传递，receipts的切片要排序
		if bytes.Compare(tx.Data()[:4], isBSSTORE) == 0 || bytes.Compare(tx.Data()[:4], isBAGG) == 0 {
			msg, err := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
			if err != nil {
				return nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
			}
			cocrTxs = append(cocrTxs, tx)
			cocrMsgs = append(cocrMsgs, &msg)
			cocrIndex = append(cocrIndex, i)
			indexToMsg[i] = &msg // 记录当前串行交易的索引
		} else {
			// 正常串行的交易
			startT := time.Now()
			msg, err := tx.AsMessage(types.MakeSigner(p.config, header.Number), header.BaseFee)
			if err != nil {
				return nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
			}
			// tx.hash 通过 LOG 指令来创造Event，i（index）用于生成收据
			statedb.SetTxContext(tx.Hash(), i)

			receipt, err := applyTransaction(msg, p.config, gp, statedb, blockNumber, blockHash, tx, usedGas, vmenv)
			if err != nil {
				return nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
			}
			//receipts = append(receipts, receipt)
			//allLogs = append(allLogs, receipt.Logs...)

			// 记录当前串行交易的索引、收据
			indexToMsg[i] = &msg
			msgToReceipt.Store(&msg, receipt)

			//计算串行执行耗时
			endT := time.Now()
			elapsed := endT.Sub(startT)
			serialTxs++
			serialTimeOverhead += elapsed
		}
	}

	// 打印串行耗时
	if serialTxs != 0 {
		fmt.Printf("\n[Lin-Record 串行]: Block: %v 执行了 %v 笔串行交易,耗时 %v . \n", block.Number(), serialTxs, serialTimeOverhead)
	}

	// 等待并发结束处理,处理receipts, allLogs, *usedGas, error , usedGas 要累加，receipt 的生成root那部分改掉
	if len(cocrMsgs) != 0 {
		cocrErrs := concurrentApplyTx(cocrMsgs, p.config, gp, statedb, blockNumber, blockHash, cocrTxs, usedGas, vmenv, &cocrWaitGroup, &msgToReceipt, &cocrIndex)
		if len(cocrErrs) != 0 {
			log.Error(fmt.Sprintf("[Lin-Process]并发交易出错: %w", cocrErrs))
			return nil, nil, 0, fmt.Errorf("[Lin-Process]并发交易出错: %w", cocrErrs)
		}
	}

	// 根据tx在区块内的顺序生成收据和日志
	for j, _ := range block.Transactions() {
		r, found := msgToReceipt.Load(indexToMsg[j])
		for !found {
			fmt.Printf("[Lin-Process-生成收据-sync.map]", found)
			r, found = msgToReceipt.Load(indexToMsg[j])
		}
		receipt := r.(*types.Receipt)
		receipts = append(receipts, receipt)
		allLogs = append(allLogs, receipt.Logs...)
	}
	//fmt.Printf("[Lin-Process]: Block %v 的 收据为: %v \n", blockNumber, receipts)

	// Fail if Shanghai not enabled and len(withdrawals) is non-zero.
	withdrawals := block.Withdrawals()
	if len(withdrawals) > 0 && !p.config.IsShanghai(block.Time()) {
		return nil, nil, 0, fmt.Errorf("withdrawals before shanghai")
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	p.engine.Finalize(p.bc, header, statedb, block.Transactions(), block.Uncles(), withdrawals)

	return receipts, allLogs, *usedGas, nil
}

func applyTransaction(msg types.Message, config *params.ChainConfig, gp *GasPool, statedb *state.StateDB, blockNumber *big.Int, blockHash common.Hash, tx *types.Transaction, usedGas *uint64, evm *vm.EVM) (*types.Receipt, error) {
	// Create a new context to be used in the EVM environment.
	txContext := NewEVMTxContext(msg)
	evm.Reset(txContext, statedb)

	// Apply the transaction to the current state (included in the env).
	result, err := ApplyMessage(evm, msg, gp)
	if err != nil {
		return nil, err
	}

	// Update the state with pending changes.
	//var root []byte
	if config.IsByzantium(blockNumber) {
		statedb.Finalise(true)
	} else {
		//root =
		statedb.IntermediateRoot(config.IsEIP158(blockNumber)).Bytes()
	}
	*usedGas += result.UsedGas

	// Create a new receipt for the transaction, storing the intermediate root and gas used
	// by the tx.
	// receipt := &types.Receipt{Type: tx.Type(), PostState: root, CumulativeGasUsed: *usedGas}
	receipt := &types.Receipt{Type: tx.Type()}
	if result.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = result.UsedGas

	// If the transaction created a contract, store the creation address in the receipt.
	if msg.To() == nil {
		receipt.ContractAddress = crypto.CreateAddress(evm.TxContext.Origin, tx.Nonce())
	}

	// Set the receipt logs and create the bloom filter.
	receipt.Logs = statedb.GetLogs(tx.Hash(), blockNumber.Uint64(), blockHash)
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = blockHash
	receipt.BlockNumber = blockNumber
	receipt.TransactionIndex = uint(statedb.TxIndex())
	return receipt, err
}

func concurrentApplyTx(msgs []*types.Message, config *params.ChainConfig, gp *GasPool, statedb *state.StateDB, blockNumber *big.Int, blockHash common.Hash, txs types.Transactions, usedGas *uint64, evm *vm.EVM, wg *sync.WaitGroup, msgToReceipt *sync.Map, cocrIndex *[]int) []error {

	// fmt.Printf("[Lin-concurrentApplyTx]: 需要并发的交易: %v \n", msgs)
	// var initGasPool = new(GasPool).AddGas(uint64(*gp)) // 记录初始gasPool
	txsIndex := *cocrIndex              // 记录并发交易在区块内部的索引
	var cocrErrs []error                // 记录并发执行交易时候出现的error
	var ggeLock sync.RWMutex            // 用于更新gaspool,gas,err时用的读写锁
	cocrCount := runtime.NumCPU()       // 最大支持并发
	c := make(chan struct{}, cocrCount) // 控制任务并发的chan
	defer close(c)

	// 遍历所有需要并发的交易msgs
	startT := time.Now()
	for i, _ := range msgs {
		wg.Add(1)
		c <- struct{}{} // 作用类似于waitgroup.Add(1)

		index := i             // 记录当前循环的索引
		cocrMsg := msgs[index] //获取要执行的msg

		go func() { // 开始并行执行单个msg
			defer wg.Done() // 通知并发组,并发完成 -1

			cocrEVM := *evm                           // 创建一个新的EVM环境
			var cocrGP = new(GasPool).AddGas(8000000) // 创建一个新的gaspool

			// Create a new context to be used in the EVM environment.
			txContext := NewEVMTxContext(cocrMsg)
			cocrEVM.Reset(txContext, statedb)

			// Apply the transaction to the current state (included in the env).
			result, err := CocrApplyMessage(&cocrEVM, cocrMsg, cocrGP)

			// 如果单笔并行交易出现错误,记录错误以供返回
			if err != nil {
				log.Error("[Lin-concurrentApplyTx]:单笔并行交易执行出错", err, "\n")
				fmt.Errorf("[Lin-concurrentApplyTx]:单笔并行交易执行出错: %v \n", err)
				ggeLock.Lock()
				cocrErrs = append(cocrErrs, err)
				ggeLock.Unlock()
				// 保存一个空收据
				receipt := &types.Receipt{}
				msgToReceipt.Store(cocrMsg, receipt)
				return
			}

			// 更新gaspool和usedGas
			ggeLock.Lock()
			gpErr := gp.SubGas(result.UsedGas)
			if gpErr != nil {
				log.Error("[Lin-concurrentApplyTx]:单笔并行交易更新GasPool出错", err, "\n")
				fmt.Println("[Lin-concurrentApplyTx]:单笔并行交易更新GasPool出错", err)
			}
			*usedGas += result.UsedGas
			ggeLock.Unlock()

			// Create a new receipt for the transaction, storing the intermediate root and gas used
			// by the tx.
			receipt := &types.Receipt{Type: txs[index].Type()}
			if result.Failed() {
				receipt.Status = types.ReceiptStatusFailed
			} else {
				receipt.Status = types.ReceiptStatusSuccessful
			}
			receipt.TxHash = txs[index].Hash()
			receipt.GasUsed = result.UsedGas

			// If the transaction created a contract, store the creation address in the receipt.
			if cocrMsg.To() == nil {
				receipt.ContractAddress = crypto.CreateAddress(evm.TxContext.Origin, txs[index].Nonce())
			}

			// Set the receipt logs and create the bloom filter.
			receipt.Logs = statedb.GetLogs(txs[index].Hash(), blockNumber.Uint64(), blockHash)
			receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
			receipt.BlockHash = blockHash
			receipt.BlockNumber = blockNumber
			receipt.TransactionIndex = uint(txsIndex[index])
			// 记录当前串行交易的收据
			msgToReceipt.Store(cocrMsg, receipt)

			<-c // 执行完毕，释放资源
		}()
	}
	// 等待所有的并发完成
	wg.Wait()
	endT := time.Now()
	elapsed := endT.Sub(startT)
	if len(msgs) != 0 {
		fmt.Printf("\n[Lin-Record 并行]: Block: %v 执行了 %v 笔并行交易,耗时 %v . \n", blockNumber, len(msgs), elapsed)
	}

	// Update the state with pending changes.
	statedb.GetLock().Lock()
	if config.IsByzantium(blockNumber) {
		statedb.Finalise(true)
	} else {
		statedb.IntermediateRoot(config.IsEIP158(blockNumber)).Bytes()
	}
	statedb.GetLock().Unlock()

	return cocrErrs
}

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func ApplyTransaction(config *params.ChainConfig, bc ChainContext, author *common.Address, gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64, cfg vm.Config) (*types.Receipt, error) {
	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number), header.BaseFee)
	if err != nil {
		return nil, err
	}
	// Create a new context to be used in the EVM environment
	blockContext := NewEVMBlockContext(header, bc, author)
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, statedb, config, cfg)
	return applyTransaction(msg, config, gp, statedb, header.Number, header.Hash(), tx, usedGas, vmenv)
}
