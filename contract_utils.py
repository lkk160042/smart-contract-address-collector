from typing import List, Dict
import asyncio
import pandas as pd
from web3 import Web3, HTTPProvider
from web3.exceptions import Web3Exception


class AsyncBatchProcessor:
    def __init__(self, limit):
        self.limit = limit
        self.func = []
        self.args = []
        self.kwargs = []

    async def add_task(self, func, *args, **kwargs):
        self.func.append(func)
        self.args.append([*args])
        self.kwargs.append({**kwargs})

        if len(self.func) >= self.limit:
            return await self.process_tasks()

    async def process_tasks(self):
        if not self.func:
            return

        tasks = []
        for i in range(len(self.func)):
            tasks.append(self.func[i](*self.args[i], **self.kwargs[i]))
        self.func = []
        self.args = []
        self.kwargs = []

        return await asyncio.gather(*tasks)


class ETHContractFunctions:
    def __init__(self, web3_object: Web3, address: str, abi: List[Dict]) -> None:
        self.contract = web3_object.eth.contract(address=address, abi=abi)

    async def __call__(self, function_name, *args):
        try:
            return self.contract.functions.__dict__[function_name](*args).call()
        except Web3Exception:
            return "NotFound"

    def __getattr__(self, function_name):
        return lambda *args: self(function_name, *args)


class Contract:
    def __init__(self, web3_object: Web3):
        self.w3 = web3_object
        self.result_df = pd.DataFrame()

    @classmethod
    def from_url(cls, http_provider) -> 'Contract':
        web3 = Web3(HTTPProvider(http_provider))
        return cls(web3)

    @staticmethod
    def _get_abi(contract_type: str) -> List[Dict]:
        assert contract_type.lower() in ['pair_factory', 'pair', 'token'], \
            "contract_type should be in ['factory', 'pair', 'token']"

        if contract_type.lower() == 'pair_factory':
            return [
                {
                    "constant": True,
                    "inputs": [
                        {"internalType": "address", "name": "", "type": "address"},
                        {"internalType": "address", "name": "", "type": "address"}
                    ],
                    "name": "getPair",
                    "outputs": [{"internalType": "address", "name": "", "type": "address"}],
                    "payable": False,
                    "stateMutability": "view",
                    "type": "function"
                },
                {
                    "constant": True,
                    "inputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
                    "name": "allPairs",
                    "outputs": [{"internalType": "address", "name": "", "type": "address"}],
                    "payable": False,
                    "stateMutability": "view",
                    "type": "function"
                },
                {
                    "constant": True,
                    "inputs": [],
                    "name": "allPairsLength",
                    "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
                    "payable": False,
                    "stateMutability": "view", "type": "function"
                },
            ]

        if contract_type.lower() == 'pair':
            return [
                {"inputs": [], "name": "token0",
                 "outputs": [{"internalType": "address", "name": "", "type": "address"}],
                 "stateMutability": "view", "type": "function"},
                {"inputs": [], "name": "token1",
                 "outputs": [{"internalType": "address", "name": "", "type": "address"}],
                 "stateMutability": "view", "type": "function"},
                {"constant": True, "inputs": [], "name": "name", "outputs": [{"name": "", "type": "string"}],
                 "payable": False,
                 "stateMutability": "view", "type": "function"},
            ]

        if contract_type.lower() == 'token':
            return [
                {"constant": True, "inputs": [], "name": "name", "outputs": [{"name": "", "type": "string"}],
                 "payable": False,
                 "stateMutability": "view", "type": "function"},
            ]

    def get_token_info(self, *args, **kwargs) -> str:
        return asyncio.run(self.async_get_token_info(*args, **kwargs))

    async def async_get_token_info(self, token_address):
        contract_functions = ETHContractFunctions(self.w3, token_address, abi=self._get_abi('token'))
        return await contract_functions.name()

    def get_pair_info(self, *args, **kwargs) -> dict:
        return asyncio.run(self.async_get_pair_info(*args, **kwargs))

    async def async_get_pair_info(self, pair_address):
        contract_functions = ETHContractFunctions(self.w3, pair_address, abi=self._get_abi('pair'))
        pair_info = await asyncio.gather(contract_functions.name(), contract_functions.token0(),
                                         contract_functions.token1())
        return {
            "pair_name": pair_info[0],
            "token0": pair_info[1],
            "token1": pair_info[2],
        }

    def get_pair_by_index(self, *args, **kwargs):
        return asyncio.run(self.async_get_pair_by_index(*args, **kwargs))

    async def async_get_pair_by_index(
            self,
            factory_contract: ETHContractFunctions = None,
            uint: int = None,
            factory_address: str = None
    ):
        if factory_contract is None and factory_address is None:
            raise ValueError("Either pair_factory_contract or pair_factory_address must be set")

        if not factory_contract:
            factory_contract = ETHContractFunctions(self.w3, factory_address,
                                                    abi=self._get_abi('pair_factory'))
        pair_address = await factory_contract.allPairs(uint)
        pair_info = await self.async_get_pair_info(pair_address)
        token0, token1 = await asyncio.gather(self.async_get_token_info(pair_info["token0"]),
                                              self.async_get_token_info(pair_info["token1"]))
        return {
            "pair_address": pair_address,
            "pair_name": pair_info["pair_name"],
            "token0_address": pair_info["token0"],
            "token0_name": token0,
            "token1_address": pair_info["token1"],
            "token1_name": token1,
        }

    def get_pair_length(self, factory_address: str):
        contract_functions = ETHContractFunctions(self.w3, factory_address, abi=self._get_abi('pair_factory'))
        return asyncio.run(contract_functions.allPairsLength())

    def get_pairs(self, *args, **kwargs):
        return asyncio.run(self.async_get_pairs(*args, **kwargs))

    async def async_get_pairs(self, factory_address: str = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f", limit=5):
        processor = AsyncBatchProcessor(limit=limit)
        contract_functions = ETHContractFunctions(self.w3, factory_address, abi=self._get_abi('pair_factory'))
        pairs_length = await contract_functions.allPairsLength()
        for i in range(pairs_length):
            results = await processor.add_task(self.async_get_pair_by_index, contract_functions, i)
            if results:
                self.result_df = self.result_df._append(self.to_dataframe(results), ignore_index=True)
                print(f"{len(results)} done")
        final_results = await processor.process_tasks()
        if final_results:
            self.result_df = self.result_df._append(self.to_dataframe(final_results), ignore_index=True)

    @staticmethod
    def to_dataframe(results):
        return pd.DataFrame({
            "pair_address": [d["pair_address"] for d in results],
            "pair_name": [d["pair_name"] for d in results],
            "token0_address": [d["token0_address"] for d in results],
            "token0_name": [d["token0_name"] for d in results],
            "token1_address": [d["token1_address"] for d in results],
            "token1_name": [d["token1_name"] for d in results],
        })


if __name__ == '__main__':
    contract = Contract.from_url("https://mainnet.infura.io/v3/7fd65cf5f4e9453aa0259e639e7c1717")
    asyncio.run(contract.async_get_pairs("0x1097053Fd2ea711dad45caCcc45EfF7548fCB362"))  # Pancake Swap v2
    contract.result_df.to_csv("PancakeSwap_v2.csv", index=False)
